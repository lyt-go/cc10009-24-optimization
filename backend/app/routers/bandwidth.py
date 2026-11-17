"""带宽控制接口：维护带宽策略，并提供增量限速切换、配额审计与调度落点同步。

调用链最外层：只负责取参、取角色与 HTTP 状态码，业务判断全部在 service/engine。
写接口（登记、动作、切换、流量回写）要求管理员角色，其他角色只读。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.bandwidth import BandwidthService, PermissionError_

router = APIRouter(prefix="/api/bandwidth", tags=["带宽控制"])

service = BandwidthService()

LIST_FIELDS = ["策略编号", "节点标识", "下载带宽上限", "上传带宽上限", "突发额度", "限速粒度秒", "优先级权重", "生效时段"]
STATUSES = ["草稿", "策略评审中", "已启用", "已暂停"]


def _role(x_user_role: str | None) -> str:
    """网关注入的调用方角色；本地缺省按管理员处理，只读角色由前端显式切换。"""
    return (x_user_role or "admin").strip()


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、策略评审中、已启用、已暂停"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按策略编号与状态过滤带宽控制列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/runtime/nodes")
def list_node_states() -> dict[str, Any]:
    """节点带宽运行态：基础/突发带宽、突发额度与已用量、流量与配额版本、在线与过期标记。"""
    items = service.node_states()
    return {"module": "bandwidth", "total": len(items), "items": items}


@router.get("/runtime/allocations")
def list_allocations(
    node_id: str | None = Query(default=None, description="按节点标识过滤分配版本"),
    batch_no: str | None = Query(default=None, description="按切换批次过滤"),
) -> dict[str, Any]:
    """节点带宽分配快照（含版本、生效速率、切换边界、冲突标记），用于确认本次处理范围。"""
    items = service.allocations(node_id=node_id, batch_no=batch_no)
    return {"module": "bandwidth", "total": len(items), "items": items}


@router.get("/runtime/batches")
def list_batches() -> dict[str, Any]:
    """切换批次审计：切换依据、受影响/未受影响节点、配额校验、冲突与优先级都可回查。"""
    items = service.batches()
    return {"module": "bandwidth", "total": len(items), "items": items}


@router.get("/runtime/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    batch = service.get_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"切换批次 {batch_no} 不存在")
    return batch


@router.get("/runtime/landing")
def landing_list(
    node_id: str | None = Query(default=None, description="按节点标识过滤调度落点清单"),
) -> dict[str, Any]:
    """调度落点侧清单：每个节点当前生效的分配版本与实际下载流量回写状态。"""
    items = service.landing_list(node_id=node_id)
    return {"module": "bandwidth", "total": len(items), "items": items}


@router.post("/runtime/switch", response_model=ActionResult)
def switch_policy(
    payload: EntryPayload,
    x_user_role: str | None = Header(default=None, description="调用角色，仅 admin 可切换"),
) -> ActionResult:
    """增量限速切换：只重算受影响节点，校验剩余配额，生成切换后的节点带宽分配。

    同一幂等键重复下发返回首次结果，不产生新版本，杜绝双重限速。
    """
    try:
        result = service.switch_policy(payload.values, operator_role=_role(x_user_role))
    except PermissionError_ as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        return ActionResult(ok=False, message=str(exc))
    if result.get("重复下发"):
        return ActionResult(ok=True, message=result["message"], entry=result["批次"])
    return ActionResult(ok=True, message=result["message"], entry=result["批次"])


@router.post("/runtime/traffic-writeback", response_model=ActionResult)
def traffic_writeback(
    payload: EntryPayload,
    x_user_role: str | None = Header(default=None, description="调度落点服务账号，要求管理员口径"),
) -> ActionResult:
    """调度落点侧回写实际下载流量，刷新突发用量与流量快照版本，供下一轮配额计算。"""
    try:
        result = service.writeback_traffic(payload.values, operator_role=_role(x_user_role))
    except PermissionError_ as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    except (ValueError, TypeError) as exc:
        return ActionResult(ok=False, message=f"流量回写参数不合法：{exc}")
    return ActionResult(ok=True, message="实际下载流量已回写，配额与生效速率已刷新", entry=result)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出带宽控制清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "bandwidth", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条带宽策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"带宽策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(
    payload: EntryPayload,
    x_user_role: str | None = Header(default=None, description="仅 admin 可登记策略"),
) -> ActionResult:
    """登记一条带宽策略，缺字段时说明原因而不是静默丢弃。"""
    try:
        entry, missing = service.create_entry(payload.values, operator_role=_role(x_user_role))
    except PermissionError_ as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="带宽策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(
    entry_id: int,
    payload: EntryPayload,
    x_user_role: str | None = Header(default=None, description="仅 admin 可执行策略动作"),
) -> ActionResult:
    """对单条带宽策略执行提交策略评审、启用策略、暂停策略；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    try:
        entry, message = service.run_action(entry_id, action, operator_role=_role(x_user_role))
    except PermissionError_ as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
