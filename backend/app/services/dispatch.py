"""请求分配业务规则：状态流转、字段校验，以及调度落点侧的带宽分配同步。

调度落点是带宽策略的执行端：
- 拉取调度落点清单（每个节点当前生效的节点带宽分配），供下载执行器按版本限速；
- 回写实际下载流量，交给带宽引擎刷新突发用量与流量快照版本，供下一轮配额计算。
"""
from __future__ import annotations

from typing import Any

from app.services.bandwidth import BandwidthService
from app.services.bandwidth_engine import DEFAULT_ROLE
from app.store import store

MODULE = "dispatch"
REQUIRED_FIELDS = ["策略编号", "目标域名", "分配算法"]
STATUS_ORDER = ["草稿", "策略验证中", "已启用", "已停用"]
ACTION_RULES = {"提交策略验证": "策略验证中", "启用策略": "已启用", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


class DispatchService:
    def __init__(self) -> None:
        self._bandwidth = BandwidthService()

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": store.next_id(MODULE)}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分配策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于请求分配可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"分配策略已{action}"

    # ---- 调度落点侧：带宽分配读取与流量回写 ------------------------------
    def fetch_bandwidth_allocations(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        """调度落点侧读取最新的节点带宽分配（切换后的版本即在此清单生效）。"""
        return self._bandwidth.landing_list(node_id=node_id)

    def report_download_traffic(
        self,
        values: dict[str, Any],
        *,
        operator_role: str = DEFAULT_ROLE,
    ) -> dict[str, Any]:
        """调度落点侧回写实际下载流量，结果进入下一轮配额计算。

        若调用方未给幂等键，这里按「节点+流量快照版本」补一个确定性键，
        同一份流量反复上报仍只算一次。
        """
        node_id = str(values.get("node_id") or "").strip()
        snapshot_version = values.get("snapshot_version")
        idempotency_key = str(values.get("idempotency_key") or "").strip()
        if not idempotency_key:
            if not node_id or snapshot_version in (None, ""):
                raise ValueError("回写流量需要 node_id 与 snapshot_version 才能生成幂等键")
            idempotency_key = f"landing-{node_id}-snap-{snapshot_version}"
        payload = dict(values)
        payload["idempotency_key"] = idempotency_key
        return self._bandwidth.writeback_traffic(payload, operator_role=operator_role)
