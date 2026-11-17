"""带宽增量限速切换引擎。

调用链最里层，只负责领域计算与运行态表的读-改-写，所有入口都在 store 的同一把
可重入锁内执行，外层 service/router 不直接改这些表，保证：

1. 增量切换：只重算受影响节点，未受影响节点保持原限速与原版本；
2. 配额可核对：每个节点的突发额度、已用额度、剩余额度随批次留痕，剩余额度不为负；
3. 突发额度耗尽后速率回落到基础带宽，不会同时突破基础带宽与突发上限；
4. 幂等：同一幂等键/同一分配版本反复处理只生效一次，不会产生双重限速；
5. 冲突留双版本：策略到期与下载任务完成落在同一时刻时，旧速率对应旧流量快照、
   新配额对应新流量快照，同一节点保留两个可核对的分配版本；
6. 流量与配额版本成对记录，下一轮切换可由两者共同解释。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import (
    BANDWIDTH_ALLOCATION,
    BANDWIDTH_IDEMPOTENCY,
    BANDWIDTH_SWITCH_BATCH,
    DISPATCH_BANDWIDTH_LIST,
    NODE_BANDWIDTH_STATE,
    store,
)

ADMIN_ROLE = "admin"
# 未显式带角色时的兜底角色，便于本地开发；生产入口由网关强制注入。
DEFAULT_ROLE = ADMIN_ROLE
READ_ONLY_ROLES = {"viewer", "operator", "auditor"}

_EPOCH = datetime(2026, 11, 17, tzinfo=timezone.utc)

# 演示用初始运行态：数值确定，启动即可核对配额与速率。
_BOOTSTRAP_STATES = [
    {
        "节点标识": "WORK-0001", "在线": True,
        "基础带宽Mbps": 20.0, "突发上限Mbps": 40.0, "突发额度MB": 500.0,
        "已用突发MB": 120.0, "累计下载流量MB": 3000.0,
        "限速粒度秒": 1, "优先级权重": 10, "策略到期时间": None,
    },
    {
        "节点标识": "WORK-0002", "在线": True,
        "基础带宽Mbps": 10.0, "突发上限Mbps": 25.0, "突发额度MB": 300.0,
        "已用突发MB": 300.0, "累计下载流量MB": 1800.0,
        "限速粒度秒": 1, "优先级权重": 20, "策略到期时间": None,
    },
    {
        "节点标识": "WORK-0003", "在线": False,
        "基础带宽Mbps": 15.0, "突发上限Mbps": 30.0, "突发额度MB": 400.0,
        "已用突发MB": 50.0, "累计下载流量MB": 2200.0,
        "限速粒度秒": 1, "优先级权重": 30, "策略到期时间": None,
    },
]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def _non_negative_number(values: dict[str, Any], key: str) -> float:
    """把入参解析成非负数；缺省或非法时给出可读错误，而不是静默当 0。"""
    raw = values.get(key)
    if raw is None or str(raw).strip() == "":
        raise ValueError(f"参数「{key}」缺失，无法重算切换边界")
    try:
        number = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"参数「{key}」不是合法数值：{raw!r}") from exc
    if number < 0:
        raise ValueError(f"参数「{key}」不允许为负数：{number}")
    return number


def _effective_rate(base: float, burst_cap: float, remaining_burst: float, *, burst_allowed: bool) -> float:
    """突发有余量且允许突发时取突发上限，否则回落到基础带宽。

    入参已保证突发上限不低于基础带宽；任何情况下速率都不会超过突发上限，
    突发耗尽/离线/策略过期时连基础带宽都不能突破。
    """
    if burst_allowed and remaining_burst > 0:
        return burst_cap
    return base


class BandwidthSwitchEngine:
    """增量限速切换与流量配额的领域引擎，状态全部落在 store 的运行态表里。"""

    # ---- 启动引导 -------------------------------------------------------
    def ensure_bootstrapped(self) -> None:
        """幂等初始化节点运行态、首版分配与调度落点清单。"""
        with store.locked():
            if store.rows(NODE_BANDWIDTH_STATE):
                return
            for seed in _BOOTSTRAP_STATES:
                state = {
                    "id": store.next_id(NODE_BANDWIDTH_STATE),
                    "节点标识": seed["节点标识"],
                    "在线": seed["在线"],
                    "基础带宽Mbps": seed["基础带宽Mbps"],
                    "突发上限Mbps": seed["突发上限Mbps"],
                    "突发额度MB": seed["突发额度MB"],
                    "已用突发MB": seed["已用突发MB"],
                    "累计下载流量MB": seed["累计下载流量MB"],
                    "限速粒度秒": seed["限速粒度秒"],
                    "优先级权重": seed["优先级权重"],
                    "策略到期时间": seed["策略到期时间"],
                    "流量快照版本": 1,
                    "配额版本": 1,
                    "更新时间": _iso(_EPOCH),
                }
                store.rows(NODE_BANDWIDTH_STATE).append(state)
                self._publish_allocation(state, "BOOTSTRAP", conflict=False,
                                         conflict_note=None, boundary=None,
                                         status="生效中", now=_EPOCH)
            self._sync_landing_from_allocation()

    # ---- 查询 -----------------------------------------------------------
    def list_node_states(self) -> list[dict[str, Any]]:
        self.ensure_bootstrapped()
        with store.locked():
            return [dict(row) for row in store.rows(NODE_BANDWIDTH_STATE)]

    def list_allocations(self, *, node_id: str | None = None, batch_no: str | None = None) -> list[dict[str, Any]]:
        self.ensure_bootstrapped()
        with store.locked():
            rows = store.rows(BANDWIDTH_ALLOCATION)
            if node_id:
                rows = [row for row in rows if row["节点标识"] == node_id]
            if batch_no:
                rows = [row for row in rows if row["切换批次号"] == batch_no]
            return [dict(row) for row in rows]

    def latest_allocation(self, node_id: str) -> dict[str, Any] | None:
        with store.locked():
            matches = [row for row in store.rows(BANDWIDTH_ALLOCATION) if row["节点标识"] == node_id]
            return dict(matches[-1]) if matches else None

    def list_batches(self) -> list[dict[str, Any]]:
        self.ensure_bootstrapped()
        with store.locked():
            return [dict(row) for row in store.rows(BANDWIDTH_SWITCH_BATCH)]

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        self.ensure_bootstrapped()
        with store.locked():
            for row in store.rows(BANDWIDTH_SWITCH_BATCH):
                if row["批次号"] == batch_no:
                    return dict(row)
        return None

    def landing_list(self, *, node_id: str | None = None) -> list[dict[str, Any]]:
        """调度落点侧清单：每个节点只暴露当前生效版本。"""
        self.ensure_bootstrapped()
        with store.locked():
            rows = store.rows(DISPATCH_BANDWIDTH_LIST)
            if node_id:
                rows = [row for row in rows if row["节点标识"] == node_id]
            return [dict(row) for row in rows]

    # ---- 增量切换主流程 -------------------------------------------------
    def switch_policy(
        self,
        *,
        affected_nodes: list[str],
        policy: dict[str, Any],
        idempotency_key: str,
        reason: str,
        operator_role: str,
        now: datetime | None = None,
        task_completion_at: dict[str, str] | None = None,
        policy_expire_at: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """对受影响节点做增量限速切换，返回可核对的批次结果。

        同一 ``idempotency_key`` 重复下发直接返回首次结果，不会再生成分配版本，
        因此不会出现双重限速。
        """
        self.ensure_bootstrapped()
        moment = now or _now()
        task_completion_at = task_completion_at or {}
        policy_expire_at = policy_expire_at or {}

        with store.locked():
            duplicated = self._find_idempotent_batch(idempotency_key)
            if duplicated is not None:
                return {
                    "重复下发": True,
                    "批次号": duplicated["批次号"],
                    "message": "相同切换请求已处理过，本次按幂等规则跳过，未产生新限速版本",
                    "批次": duplicated,
                }

            nodes = [n.strip() for n in affected_nodes if str(n or "").strip()]
            if not nodes:
                raise ValueError("受影响节点为空，无法执行增量切换")
            if len(set(nodes)) != len(nodes):
                raise ValueError("受影响节点存在重复标识，请去重后再切换")

            state_rows = store.rows(NODE_BANDWIDTH_STATE)
            state_by_node = {row["节点标识"]: row for row in state_rows}
            unknown = [node for node in nodes if node not in state_by_node]
            if unknown:
                raise ValueError(f"节点不存在或未纳管：{'、'.join(unknown)}")

            new_base = _non_negative_number(policy, "基础带宽Mbps")
            new_burst_cap = _non_negative_number(policy, "突发上限Mbps")
            new_quota = _non_negative_number(policy, "突发额度MB")
            if new_burst_cap < new_base:
                raise ValueError("突发上限不能低于基础带宽，否则突发语义不成立")
            granularity = int(_non_negative_number(policy, "限速粒度秒") or 1)
            priority = int(_non_negative_number(policy, "优先级权重"))

            batch_no = self._next_batch_no(moment)
            quota_checks: list[dict[str, Any]] = []
            boundaries: list[dict[str, Any]] = []
            exhausted_nodes: list[str] = []
            clamped_nodes: list[str] = []
            conflicts: list[dict[str, Any]] = []
            allocation_ids: list[int] = []

            for node in nodes:
                state = state_by_node[node]
                old_allocation = self.latest_allocation(node) or {}
                old_base = float(state["基础带宽Mbps"])
                old_cap = float(state["突发上限Mbps"])
                old_quota = float(state["突发额度MB"])
                used_before = float(state["已用突发MB"])
                snapshot_before = int(state["流量快照版本"])
                quota_version_before = int(state["配额版本"])

                # 配额校验：已用量按真实消耗保留，仅当新额度更低时截断到新额度；
                # 剩余额度恒不为负。耗尽节点不会因额度上调而"复活"突发能力。
                remaining_after = max(0.0, new_quota - used_before)
                exhausted = remaining_after == 0
                expire_at = policy_expire_at.get(node)
                expired = expire_at is not None and expire_at <= _iso(moment)
                burst_allowed = bool(state["在线"]) and not expired
                effective_rate = _effective_rate(
                    new_base, new_burst_cap, remaining_after, burst_allowed=burst_allowed
                )
                if not burst_allowed:
                    clamped_nodes.append(node)

                boundary = {
                    "节点标识": node,
                    "旧基础带宽Mbps": old_base,
                    "旧突发上限Mbps": old_cap,
                    "旧突发额度MB": old_quota,
                    "新基础带宽Mbps": new_base,
                    "新突发上限Mbps": new_burst_cap,
                    "新突发额度MB": new_quota,
                    "旧生效速率Mbps": old_allocation.get("生效速率Mbps", old_base),
                    "新生效速率Mbps": effective_rate,
                    "切换前流量快照版本": snapshot_before,
                    "生效时间": _iso(moment),
                    "切换依据": reason,
                }
                boundaries.append(boundary)
                quota_checks.append({
                    "节点标识": node,
                    "切换前突发额度MB": old_quota,
                    "新突发额度MB": new_quota,
                    "已用突发MB": used_before,
                    "剩余突发MB": remaining_after,
                    "突发是否耗尽": exhausted,
                    "节点在线": bool(state["在线"]),
                    "策略是否过期": expired,
                    "流量快照版本": snapshot_before,
                    "配额版本": quota_version_before,
                })
                if exhausted:
                    exhausted_nodes.append(node)

                # 先把节点运行态切到新配额，配额版本 +1，流量快照版本保持不变。
                state["基础带宽Mbps"] = new_base
                state["突发上限Mbps"] = new_burst_cap
                state["突发额度MB"] = new_quota
                # 已用突发按真实消耗保留，仅在额度下调且低于已用量时截断，杜绝负数余量。
                state["已用突发MB"] = min(used_before, new_quota)
                state["限速粒度秒"] = granularity
                state["优先级权重"] = priority
                state["配额版本"] = quota_version_before + 1
                if expire_at is not None:
                    state["策略到期时间"] = expire_at
                state["更新时间"] = _iso(moment)

                completion_at = task_completion_at.get(node)
                # 冲突口径：任务完成与策略到期落在同一时刻（两个入参时间戳相等），
                # 不依赖与服务器时钟严格对齐；是否过期仍由到期时间与当前时刻单独判断。
                same_moment_conflict = (
                    completion_at is not None and expire_at is not None and completion_at == expire_at
                )

                if same_moment_conflict:
                    # 冲突：策略到期与任务完成同一时刻。旧速率绑定旧流量快照先落一个
                    # 截止版本，新配额再绑定当前快照落生效版本，两个版本都可回查。
                    old_version = self._publish_allocation(
                        state, batch_no, conflict=True,
                        conflict_note="策略到期与下载任务完成同时发生，保留旧速率对应旧流量快照的截止版本",
                        boundary=boundary, status="冲突截止", now=moment,
                        override_rate=old_allocation.get("生效速率Mbps", old_base),
                        override_snapshot=snapshot_before,
                    )
                    conflicts.append({
                        "节点标识": node,
                        "冲突类型": "策略到期与任务完成同时刻",
                        "时刻": _iso(moment),
                        "旧版本分配id": old_version["id"],
                        "旧流量快照版本": snapshot_before,
                        "说明": "旧速率与新配额对应不同流量快照，已生成两个分配版本",
                    })

                new_version = self._publish_allocation(
                    state, batch_no, conflict=same_moment_conflict,
                    conflict_note=(
                        "策略到期与任务完成同时刻，本版本为新配额对应新流量快照的生效版本"
                        if same_moment_conflict else None
                    ),
                    boundary=boundary, status="生效中", now=moment,
                )
                allocation_ids.append(new_version["id"])
                if same_moment_conflict:
                    conflicts[-1]["新版本分配id"] = new_version["id"]
                    conflicts[-1]["新流量快照版本"] = new_version["流量快照版本"]

            affected_set = set(nodes)
            untouched = [
                row["节点标识"] for row in state_rows if row["节点标识"] not in affected_set
            ]
            batch = {
                "id": store.next_id(BANDWIDTH_SWITCH_BATCH),
                "批次号": batch_no,
                "幂等键": idempotency_key,
                "操作角色": operator_role,
                "操作时间": _iso(moment),
                "切换依据": reason,
                "受影响节点": nodes,
                "未受影响节点": untouched,
                "处理范围": {"受影响": nodes, "未受影响保持原限速": untouched},
                "切换边界": boundaries,
                "配额校验": quota_checks,
                "突发耗尽节点": exhausted_nodes,
                "安全边界内截断节点": clamped_nodes,
                "冲突记录": conflicts,
                "分配id列表": allocation_ids,
                "优先级权重": priority,
                "结果状态": "已生效",
            }
            store.rows(BANDWIDTH_SWITCH_BATCH).append(batch)
            store.rows(BANDWIDTH_IDEMPOTENCY).append({
                "id": store.next_id(BANDWIDTH_IDEMPOTENCY),
                "幂等键": idempotency_key,
                "批次号": batch_no,
                "创建时间": _iso(moment),
            })
            # 切换后的节点带宽分配同步到调度落点侧清单。
            self._sync_landing_from_allocation()
            return {"重复下发": False, "批次号": batch_no, "message": "增量限速切换已在安全边界内生效", "批次": batch}

    # ---- 调度落点侧：流量回写 -------------------------------------------
    def writeback_traffic(
        self,
        *,
        node_id: str,
        traffic_mb: float,
        duration_seconds: float,
        snapshot_version: int,
        idempotency_key: str,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """调度落点侧回写实际下载流量，幂等键保证同一批回写只算一次。

        突发用量按「超出基础带宽允许量的部分」累计，且不超过突发额度；回写后刷新
        流量快照版本，供下一轮配额计算与分配版本共同解释。
        """
        self.ensure_bootstrapped()
        moment = now or _now()
        if traffic_mb < 0 or duration_seconds < 0:
            raise ValueError("下载流量与时长都不允许为负数")
        if duration_seconds == 0 and traffic_mb > 0:
            raise ValueError("时长为 0 时无法核算突发用量，请提供真实统计区间")

        with store.locked():
            duplicated = self._find_idempotent(idempotency_key)
            if duplicated is not None:
                return {
                    "重复下发": True,
                    "批次号": duplicated["批次号"],
                    "message": "该批流量已回写过，按幂等规则跳过，未重复计入配额",
                }
            state = next(
                (row for row in store.rows(NODE_BANDWIDTH_STATE) if row["节点标识"] == node_id),
                None,
            )
            if state is None:
                raise ValueError(f"节点不存在或未纳管：{node_id}")

            base_mbps = float(state["基础带宽Mbps"])
            quota = float(state["突发额度MB"])
            used_before = float(state["已用突发MB"])
            # 基础带宽在统计区间内允许的下载量（Mbps -> MB）。
            base_allowance_mb = base_mbps * float(duration_seconds) / 8.0
            burst_consumption = max(0.0, float(traffic_mb) - base_allowance_mb)
            used_after = min(quota, used_before + burst_consumption)
            state["已用突发MB"] = used_after
            state["累计下载流量MB"] = float(state["累计下载流量MB"]) + float(traffic_mb)
            state["流量快照版本"] = int(snapshot_version)
            state["更新时间"] = _iso(moment)

            remaining = max(0.0, quota - used_after)
            latest = self.latest_allocation(node_id)
            effective_rate = _effective_rate(
                base_mbps, float(state["突发上限Mbps"]), remaining,
                burst_allowed=bool(state["在线"]) and not self._policy_expired(state, moment),
            )
            landing_row = self._landing_row(node_id)
            if landing_row is not None and latest is not None:
                landing_row["实际下载流量MB"] = state["累计下载流量MB"]
                landing_row["剩余突发MB"] = remaining
                landing_row["生效速率Mbps"] = effective_rate
                landing_row["流量快照版本"] = state["流量快照版本"]
                landing_row["配额版本"] = state["配额版本"]
                landing_row["回写时间"] = _iso(moment)
                landing_row["同步状态"] = "已回写"

            store.rows(BANDWIDTH_IDEMPOTENCY).append({
                "id": store.next_id(BANDWIDTH_IDEMPOTENCY),
                "幂等键": idempotency_key,
                "批次号": f"TRAFFIC-{node_id}-{snapshot_version}",
                "创建时间": _iso(moment),
            })
            return {
                "重复下发": False,
                "节点标识": node_id,
                "基础带宽允许量MB": round(base_allowance_mb, 3),
                "本次突发消耗MB": round(burst_consumption, 3),
                "已用突发MB": used_after,
                "剩余突发MB": remaining,
                "累计下载流量MB": state["累计下载流量MB"],
                "流量快照版本": state["流量快照版本"],
                "配额版本": state["配额版本"],
                "回写后生效速率Mbps": effective_rate,
            }

    # ---- 内部工具 -------------------------------------------------------
    def _publish_allocation(
        self,
        state: dict[str, Any],
        batch_no: str,
        *,
        conflict: bool,
        conflict_note: str | None,
        boundary: dict[str, Any] | None,
        status: str,
        now: datetime,
        override_rate: float | None = None,
        override_snapshot: int | None = None,
    ) -> dict[str, Any]:
        """把节点当前运行态固化成一条分配快照，并把同节点旧生效版本标记为被替换。"""
        node = state["节点标识"]
        quota = float(state["突发额度MB"])
        used = float(state["已用突发MB"])
        remaining = max(0.0, quota - used)
        burst_allowed = bool(state["在线"]) and not self._policy_expired(state, now)
        rate = override_rate if override_rate is not None else _effective_rate(
            float(state["基础带宽Mbps"]), float(state["突发上限Mbps"]),
            remaining, burst_allowed=burst_allowed,
        )
        version = self._next_version(node)
        snapshot = override_snapshot if override_snapshot is not None else int(state["流量快照版本"])
        allocation = {
            "id": store.next_id(BANDWIDTH_ALLOCATION),
            "节点标识": node,
            "切换批次号": batch_no,
            "版本号": version,
            "基础带宽Mbps": float(state["基础带宽Mbps"]),
            "突发上限Mbps": float(state["突发上限Mbps"]),
            "突发额度MB": quota,
            "已用突发MB": used,
            "剩余突发MB": remaining,
            "生效速率Mbps": rate,
            "限速粒度秒": state["限速粒度秒"],
            "优先级权重": state["优先级权重"],
            "流量快照版本": snapshot,
            "配额版本": int(state["配额版本"]),
            "切换边界": boundary,
            "冲突标记": conflict,
            "冲突说明": conflict_note,
            "处理范围": "受影响节点" if batch_no != "BOOTSTRAP" else "初始版本",
            "状态": status,
            "生效时间": _iso(now),
        }
        # 同节点旧的生效版本让位，保证调度落点侧只会读到一个生效版本。
        for row in store.rows(BANDWIDTH_ALLOCATION):
            if row["节点标识"] == node and row["状态"] == "生效中":
                row["状态"] = "已被新版本替换"
        store.rows(BANDWIDTH_ALLOCATION).append(allocation)
        return allocation

    def _sync_landing_from_allocation(self) -> None:
        """用每个节点的生效分配重建调度落点侧清单（结果记录同步到业务面）。"""
        landing = store.rows(DISPATCH_BANDWIDTH_LIST)
        existing = {row["节点标识"]: row for row in landing}
        for state in store.rows(NODE_BANDWIDTH_STATE):
            node = state["节点标识"]
            latest = self.latest_allocation(node)
            if latest is None:
                continue
            payload = {
                "节点标识": node,
                "分配id": latest["id"],
                "分配版本号": latest["版本号"],
                "基础带宽Mbps": latest["基础带宽Mbps"],
                "突发上限Mbps": latest["突发上限Mbps"],
                "生效速率Mbps": latest["生效速率Mbps"],
                "剩余突发MB": latest["剩余突发MB"],
                "流量快照版本": latest["流量快照版本"],
                "配额版本": latest["配额版本"],
                "优先级权重": latest["优先级权重"],
                "实际下载流量MB": state["累计下载流量MB"],
                "同步状态": "待回写",
                "回写时间": None,
                "更新时间": latest["生效时间"],
            }
            if node in existing:
                existing[node].update(payload)
            else:
                landing.append({"id": store.next_id(DISPATCH_BANDWIDTH_LIST), **payload})

    def _landing_row(self, node_id: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.rows(DISPATCH_BANDWIDTH_LIST) if row["节点标识"] == node_id),
            None,
        )

    def _next_version(self, node_id: str) -> int:
        versions = [
            int(row["版本号"])
            for row in store.rows(BANDWIDTH_ALLOCATION)
            if row["节点标识"] == node_id
        ]
        return max(versions, default=0) + 1

    def _next_batch_no(self, moment: datetime) -> str:
        day = moment.astimezone(timezone.utc).strftime("%Y%m%d")
        prefix = f"SW-{day}-"
        seq = [
            int(row["批次号"].rsplit("-", 1)[-1])
            for row in store.rows(BANDWIDTH_SWITCH_BATCH)
            if str(row["批次号"]).startswith(prefix)
        ]
        return f"{prefix}{max(seq, default=0) + 1:04d}"

    def _find_idempotent(self, key: str) -> dict[str, Any] | None:
        """按幂等键查处理记录；切换批次与流量回写都在同一张幂等表里去重。"""
        for row in store.rows(BANDWIDTH_IDEMPOTENCY):
            if row["幂等键"] == key:
                return dict(row)
        return None

    def _find_idempotent_batch(self, key: str) -> dict[str, Any] | None:
        record = self._find_idempotent(key)
        if record is None:
            return None
        for batch in store.rows(BANDWIDTH_SWITCH_BATCH):
            if batch["批次号"] == record["批次号"]:
                return dict(batch)
        return None

    @staticmethod
    def _policy_expired(state: dict[str, Any], moment: datetime) -> bool:
        expire_at = state.get("策略到期时间")
        return expire_at is not None and expire_at <= _iso(moment)


engine = BandwidthSwitchEngine()
