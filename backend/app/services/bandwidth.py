"""带宽控制业务规则：状态流转、字段校验、增量限速切换与权限编排。

调用链：router（取角色/参数）-> service（权限与入参校验）-> bandwidth_engine
（增量切换领域计算）-> store（带锁运行态表）。
权限口径：管理员可写（登记/动作/切换/回写），其他角色只读，写操作一律拒绝。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.bandwidth_engine import (
    ADMIN_ROLE,
    DEFAULT_ROLE,
    engine,
)
from app.store import store

MODULE = "bandwidth"
REQUIRED_FIELDS = ["策略编号", "节点标识", "下载带宽上限"]
STATUS_ORDER = ["草稿", "策略评审中", "已启用", "已暂停"]
ACTION_RULES = {"提交策略评审": "策略评审中", "启用策略": "已启用", "暂停策略": "已暂停"}
NEGATIVE_ACTIONS = []

# 切换时允许携带的策略字段，service 负责做白名单过滤，避免脏字段进引擎。
SWITCH_POLICY_FIELDS = ["基础带宽Mbps", "突发上限Mbps", "突发额度MB", "限速粒度秒", "优先级权重"]
SWITCH_REQUIRED_POLICY = ["基础带宽Mbps", "突发上限Mbps", "突发额度MB"]


class PermissionError_(Exception):
    """角色不具备写权限时抛出，由路由层转成 403。"""


class BandwidthService:
    # ---- 权限 -----------------------------------------------------------
    @staticmethod
    def _require_admin(operator_role: str) -> None:
        if operator_role != ADMIN_ROLE:
            raise PermissionError_(
                f"角色「{operator_role}」对带宽策略只有只读权限，节点归属与限速参数仅管理员可改"
            )

    # ---- 既有清单能力 ---------------------------------------------------
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

    def create_entry(self, values: dict[str, Any], *, operator_role: str = DEFAULT_ROLE) -> tuple[dict[str, Any] | None, list[str]]:
        self._require_admin(operator_role)
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

    def run_action(self, entry_id: int, action: str, *, operator_role: str = DEFAULT_ROLE) -> tuple[dict[str, Any] | None, str]:
        self._require_admin(operator_role)
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"带宽策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于带宽控制可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"带宽策略已{action}"

    # ---- 增量限速切换（调用链第二段：编排与校验） ------------------------
    def node_states(self) -> list[dict[str, Any]]:
        return engine.list_node_states()

    def allocations(self, *, node_id: str | None = None, batch_no: str | None = None) -> list[dict[str, Any]]:
        return engine.list_allocations(node_id=node_id, batch_no=batch_no)

    def batches(self) -> list[dict[str, Any]]:
        return engine.list_batches()

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        return engine.get_batch(batch_no)

    def landing_list(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        return engine.landing_list(node_id=node_id)

    def switch_policy(
        self,
        values: dict[str, Any],
        *,
        operator_role: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        self._require_admin(operator_role)
        idempotency_key = str(values.get("idempotency_key") or "").strip()
        if not idempotency_key:
            raise ValueError("缺少幂等键 idempotency_key，重复下发将无法去重")
        affected = values.get("affected_nodes") or []
        if not isinstance(affected, list) or not affected:
            raise ValueError("受影响节点 affected_nodes 必须是非空数组")
        raw_policy = values.get("policy") or {}
        if not isinstance(raw_policy, dict):
            raise ValueError("policy 必须是带宽参数对象")
        policy = {field: raw_policy.get(field) for field in SWITCH_POLICY_FIELDS}
        missing = [field for field in SWITCH_REQUIRED_POLICY if policy.get(field) in (None, "")]
        if missing:
            raise ValueError(f"切换策略缺少必填带宽参数：{'、'.join(missing)}")
        if policy.get("限速粒度秒") in (None, ""):
            policy["限速粒度秒"] = 1
        if policy.get("优先级权重") in (None, ""):
            policy["优先级权重"] = 0
        reason = str(values.get("reason") or "").strip() or "管理员触发带宽策略增量切换"
        return engine.switch_policy(
            affected_nodes=[str(n) for n in affected],
            policy=policy,
            idempotency_key=idempotency_key,
            reason=reason,
            operator_role=operator_role,
            now=now,
            task_completion_at=values.get("task_completion_at") or {},
            policy_expire_at=values.get("policy_expire_at") or {},
        )

    def writeback_traffic(self, values: dict[str, Any], *, operator_role: str) -> dict[str, Any]:
        # 流量回写来自调度落点侧服务账号，同样限定管理员口径，防止普通角色伪造流量。
        self._require_admin(operator_role)
        node_id = str(values.get("node_id") or "").strip()
        if not node_id:
            raise ValueError("缺少 node_id，无法回写实际下载流量")
        idempotency_key = str(values.get("idempotency_key") or "").strip()
        if not idempotency_key:
            raise ValueError("缺少幂等键 idempotency_key，重复回写将无法去重")
        traffic_mb = float(values.get("traffic_mb"))
        duration_seconds = float(values.get("duration_seconds"))
        snapshot_version = int(values.get("snapshot_version"))
        return engine.writeback_traffic(
            node_id=node_id,
            traffic_mb=traffic_mb,
            duration_seconds=duration_seconds,
            snapshot_version=snapshot_version,
            idempotency_key=idempotency_key,
        )
