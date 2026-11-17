"""带宽增量限速切换的领域约束测试。

与应用代码隔离：本文件放在独立目录，用 `python -I` 运行，仅在脚本内显式加入后端路径。
覆盖：增量范围、配额非负、突发耗尽回落、幂等、并发不分叉、冲突双版本、
离线/过期安全边界、流量回写、管理员写权限、调度落点联动。
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest import TestCase, main

sys.path.insert(0, "/workspace/backend")

from app.services.bandwidth import BandwidthService, PermissionError_  # noqa: E402
from app.services.bandwidth_engine import engine  # noqa: E402
from app.services.dispatch import DispatchService  # noqa: E402
from app.store import (  # noqa: E402
    BANDWIDTH_ALLOCATION,
    BANDWIDTH_IDEMPOTENCY,
    BANDWIDTH_SWITCH_BATCH,
    BANDWIDTH_RUNTIME_TABLES,
    DISPATCH_BANDWIDTH_LIST,
    NODE_BANDWIDTH_STATE,
    store,
)

MOMENT = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)
MOMENT_ISO = MOMENT.isoformat()

POLICY = {"基础带宽Mbps": 25, "突发上限Mbps": 50, "突发额度MB": 600, "限速粒度秒": 1, "优先级权重": 10}


def reset_runtime() -> None:
    """清空带宽运行态表并重新引导，保证每个用例相互独立。"""
    for table in BANDWIDTH_RUNTIME_TABLES:
        store.rows(table).clear()
    engine.ensure_bootstrapped()


def node_state(node_id: str) -> dict:
    return next(row for row in store.rows(NODE_BANDWIDTH_STATE) if row["节点标识"] == node_id)


def allocations_of(node_id: str) -> list[dict]:
    return [row for row in store.rows(BANDWIDTH_ALLOCATION) if row["节点标识"] == node_id]


def active_allocation(node_id: str) -> dict:
    active = [row for row in allocations_of(node_id) if row["状态"] == "生效中"]
    assert len(active) == 1, f"{node_id} 生效中版本应恰好一个，实际 {len(active)}"
    return active[0]


class BandwidthSwitchTests(TestCase):
    def setUp(self) -> None:
        reset_runtime()
        self.service = BandwidthService()
        self.dispatch = DispatchService()

    # 1. 引导：初始分配与调度落点清单一一对应 --------------------------------
    def test_bootstrap_syncs_landing(self) -> None:
        self.assertEqual(len(store.rows(NODE_BANDWIDTH_STATE)), 3)
        self.assertEqual(len(store.rows(BANDWIDTH_ALLOCATION)), 3)
        landing = engine.landing_list()
        self.assertEqual({row["节点标识"] for row in landing}, {"WORK-0001", "WORK-0002", "WORK-0003"})
        self.assertTrue(all(row["同步状态"] == "待回写" for row in landing))

    # 2. 增量切换：只处理受影响节点，未受影响节点保持原限速与原版本 -----------
    def test_incremental_switch_scope(self) -> None:
        before_2 = active_allocation("WORK-0002")
        before_3 = active_allocation("WORK-0003")
        result = self.service.switch_policy(
            {"idempotency_key": "k1", "affected_nodes": ["WORK-0001"], "policy": POLICY, "reason": "夜间提速"},
            operator_role="admin", now=MOMENT,
        )
        batch = result["批次"]
        self.assertEqual(batch["受影响节点"], ["WORK-0001"])
        self.assertEqual(set(batch["未受影响节点"]), {"WORK-0002", "WORK-0003"})
        self.assertEqual(batch["切换依据"], "夜间提速")
        self.assertEqual(batch["优先级权重"], 10)

        active_1 = active_allocation("WORK-0001")
        self.assertEqual(active_1["基础带宽Mbps"], 25.0)
        self.assertEqual(active_1["突发上限Mbps"], 50.0)
        self.assertEqual(active_1["处理范围"], "受影响节点")
        # 旧版本被标记替换，新版本唯一生效
        self.assertEqual(allocations_of("WORK-0001")[0]["状态"], "已被新版本替换")
        # 未受影响节点的分配 id 与版本完全不变
        self.assertEqual(active_allocation("WORK-0002")["id"], before_2["id"])
        self.assertEqual(active_allocation("WORK-0003")["id"], before_3["id"])
        # 调度落点清单同步到新版本
        landing_1 = next(row for row in engine.landing_list() if row["节点标识"] == "WORK-0001")
        self.assertEqual(landing_1["分配id"], active_1["id"])

    # 3. 配额校验：新额度小于已用量时截断为 0，不允许负数余量 ------------------
    def test_quota_never_negative(self) -> None:
        self.assertEqual(node_state("WORK-0002")["已用突发MB"], 300.0)
        small_quota = {**POLICY, "突发额度MB": 100}
        self.service.switch_policy(
            {"idempotency_key": "k2", "affected_nodes": ["WORK-0002"], "policy": small_quota},
            operator_role="admin", now=MOMENT,
        )
        state = node_state("WORK-0002")
        self.assertEqual(state["已用突发MB"], 100.0)
        self.assertEqual(active_allocation("WORK-0002")["剩余突发MB"], 0.0)
        check = engine.list_batches()[-1]["配额校验"][0]
        self.assertTrue(check["突发是否耗尽"])
        self.assertGreaterEqual(check["剩余突发MB"], 0.0)

    # 3b. 额度上调不复活已耗尽节点：已用量按真实消耗保留，仍回落基础带宽 ---------
    def test_quota_increase_keeps_exhaustion(self) -> None:
        self.assertEqual(node_state("WORK-0002")["已用突发MB"], 300.0)
        bigger_quota = {**POLICY, "突发额度MB": 900}  # 额度高于已用量
        self.service.switch_policy(
            {"idempotency_key": "k2b", "affected_nodes": ["WORK-0002"], "policy": bigger_quota},
            operator_role="admin", now=MOMENT,
        )
        state = node_state("WORK-0002")
        # 已用量原样保留，剩余 = 900 - 300
        self.assertEqual(state["已用突发MB"], 300.0)
        self.assertEqual(active_allocation("WORK-0002")["剩余突发MB"], 600.0)
        # 有剩余额度后允许突发（在线、未过期），速率取突发上限
        self.assertEqual(active_allocation("WORK-0002")["生效速率Mbps"], 50.0)

    # 4. 突发耗尽：新额度低于已用量时速率回落基础带宽，不能同时突破两个上限 ------
    def test_burst_exhausted_falls_back_to_base(self) -> None:
        # WORK-0002 已用 300，新额度 100 低于已用量 -> 剩余 0，回落基础带宽
        policy = {"基础带宽Mbps": 12, "突发上限Mbps": 30, "突发额度MB": 100}
        self.service.switch_policy(
            {"idempotency_key": "k3", "affected_nodes": ["WORK-0002"], "policy": policy},
            operator_role="admin", now=MOMENT,
        )
        rate = active_allocation("WORK-0002")["生效速率Mbps"]
        self.assertEqual(rate, 12.0)
        self.assertLessEqual(rate, 12.0)

    # 5. 突发有余量：取突发上限，但永不超过突发上限 ----------------------------
    def test_burst_available_uses_cap(self) -> None:
        self.service.switch_policy(
            {"idempotency_key": "k4", "affected_nodes": ["WORK-0001"], "policy": POLICY},
            operator_role="admin", now=MOMENT,
        )
        self.assertEqual(active_allocation("WORK-0001")["生效速率Mbps"], 50.0)

    # 6. 幂等：重复下发不产生新版本，不会双重限速 ------------------------------
    def test_switch_idempotent(self) -> None:
        payload = {"idempotency_key": "dup", "affected_nodes": ["WORK-0001"], "policy": POLICY}
        first = self.service.switch_policy(payload, operator_role="admin", now=MOMENT)
        versions_after_first = len(allocations_of("WORK-0001"))
        second = self.service.switch_policy(payload, operator_role="admin", now=MOMENT)
        self.assertTrue(second["重复下发"])
        self.assertEqual(second["批次号"], first["批次号"])
        self.assertEqual(len(allocations_of("WORK-0001")), versions_after_first)
        self.assertEqual(len(store.rows(BANDWIDTH_SWITCH_BATCH)), 1)
        self.assertEqual(len(store.rows(BANDWIDTH_IDEMPOTENCY)), 1)
        self.assertEqual(len(active_allocation("WORK-0001") and [active_allocation("WORK-0001")]), 1)

    # 7. 并发：同一幂等键并发只生效一次；不同键并发版本不分叉 ------------------
    def test_concurrent_switch_no_fork(self) -> None:
        barrier = threading.Barrier(10)

        def same_key(_: int) -> dict:
            barrier.wait()
            return self.service.switch_policy(
                {"idempotency_key": "race-same", "affected_nodes": ["WORK-0001"], "policy": POLICY},
                operator_role="admin", now=MOMENT,
            )

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(same_key, range(10)))
        self.assertEqual(sum(1 for r in results if not r["重复下发"]), 1)
        self.assertEqual(sum(1 for r in results if r["重复下发"]), 9)

        def distinct_key(i: int) -> dict:
            return self.service.switch_policy(
                {"idempotency_key": f"race-{i}", "affected_nodes": ["WORK-0001"],
                 "policy": {**POLICY, "基础带宽Mbps": 25 + i}},
                operator_role="admin",
            )

        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(distinct_key, range(20)))
        node_allocations = allocations_of("WORK-0001")
        versions = [row["版本号"] for row in node_allocations]
        self.assertEqual(len(versions), len(set(versions)), "版本号出现重复，结果分叉")
        self.assertEqual(versions, sorted(versions))
        self.assertEqual(len([row for row in node_allocations if row["状态"] == "生效中"]), 1)

    # 8. 冲突：策略到期与任务完成同时刻，同节点保留两个可核对版本 ---------------
    def test_conflict_keeps_two_versions(self) -> None:
        result = self.service.switch_policy(
            {"idempotency_key": "conflict-1", "affected_nodes": ["WORK-0001"], "policy": POLICY,
             "task_completion_at": {"WORK-0001": MOMENT_ISO},
             "policy_expire_at": {"WORK-0001": MOMENT_ISO}},
            operator_role="admin", now=MOMENT,
        )
        batch = result["批次"]
        self.assertEqual(len(batch["冲突记录"]), 1)
        conflict = batch["冲突记录"][0]
        self.assertIn("旧速率与新配额对应不同流量快照", conflict["说明"])
        node_rows = [row for row in allocations_of("WORK-0001") if row["切换批次号"] == batch["批次号"]]
        self.assertEqual(len(node_rows), 2)
        cutoff, active = node_rows
        self.assertEqual(cutoff["状态"], "冲突截止")
        self.assertTrue(cutoff["冲突标记"])
        self.assertEqual(active["状态"], "生效中")
        self.assertEqual(cutoff["版本号"] + 1, active["版本号"])
        self.assertEqual(cutoff["流量快照版本"], conflict["旧流量快照版本"])
        self.assertEqual(active["流量快照版本"], conflict["新流量快照版本"])
        # 同时刻策略已过期：新版本按安全边界回落到基础带宽
        self.assertEqual(active["生效速率Mbps"], 25.0)

    # 9. 节点离线：安全边界生效，速率不超过基础带宽，不留负数 ------------------
    def test_offline_node_clamped(self) -> None:
        self.assertFalse(node_state("WORK-0003")["在线"])
        self.service.switch_policy(
            {"idempotency_key": "offline-1", "affected_nodes": ["WORK-0003"], "policy": POLICY},
            operator_role="admin", now=MOMENT,
        )
        batch = engine.list_batches()[-1]
        self.assertIn("WORK-0003", batch["安全边界内截断节点"])
        self.assertEqual(active_allocation("WORK-0003")["生效速率Mbps"], 25.0)
        self.assertGreaterEqual(active_allocation("WORK-0003")["剩余突发MB"], 0.0)

    # 10. 策略过期：即使突发有余量也不能突破基础带宽 ---------------------------
    def test_expired_policy_clamped(self) -> None:
        self.service.switch_policy(
            {"idempotency_key": "expire-1", "affected_nodes": ["WORK-0001"], "policy": POLICY,
             "policy_expire_at": {"WORK-0001": MOMENT_ISO}},
            operator_role="admin", now=MOMENT,
        )
        self.assertEqual(active_allocation("WORK-0001")["生效速率Mbps"], 25.0)
        self.assertTrue(engine.list_batches()[-1]["配额校验"][0]["策略是否过期"])

    # 11. 流量回写：突发按超出基础允许量累计，刷新快照并同步落点 ---------------
    def test_traffic_writeback_and_landing_sync(self) -> None:
        result = self.service.writeback_traffic(
            {"node_id": "WORK-0001", "traffic_mb": 400, "duration_seconds": 60,
             "snapshot_version": 7, "idempotency_key": "wb-1"},
            operator_role="admin",
        )
        # 基础允许量 = 20Mbps * 60s / 8 = 150MB；突发消耗 = 400-150 = 250MB
        self.assertEqual(result["基础带宽允许量MB"], 150.0)
        self.assertEqual(result["本次突发消耗MB"], 250.0)
        self.assertEqual(result["已用突发MB"], 370.0)  # 120 + 250
        self.assertEqual(result["剩余突发MB"], 130.0)  # 500 - 370
        self.assertEqual(result["流量快照版本"], 7)
        landing = next(row for row in engine.landing_list() if row["节点标识"] == "WORK-0001")
        self.assertEqual(landing["同步状态"], "已回写")
        self.assertEqual(landing["流量快照版本"], 7)
        self.assertEqual(landing["实际下载流量MB"], 3400.0)
        # 重复回写同一幂等键只算一次
        again = self.service.writeback_traffic(
            {"node_id": "WORK-0001", "traffic_mb": 400, "duration_seconds": 60,
             "snapshot_version": 7, "idempotency_key": "wb-1"},
            operator_role="admin",
        )
        self.assertTrue(again["重复下发"])
        self.assertEqual(node_state("WORK-0001")["已用突发MB"], 370.0)

    # 12. 流量回写超额：已用突发封顶到额度，剩余恒非负 -------------------------
    def test_traffic_writeback_capped_at_quota(self) -> None:
        result = self.service.writeback_traffic(
            {"node_id": "WORK-0001", "traffic_mb": 100000, "duration_seconds": 10,
             "snapshot_version": 8, "idempotency_key": "wb-2"},
            operator_role="admin",
        )
        self.assertEqual(result["已用突发MB"], 500.0)
        self.assertEqual(result["剩余突发MB"], 0.0)
        self.assertEqual(result["回写后生效速率Mbps"], 20.0)  # 耗尽后回落基础带宽

    # 13. 权限：非管理员只读，所有写操作被拒；读操作放行 -----------------------
    def test_role_permission(self) -> None:
        payload = {"idempotency_key": "perm-1", "affected_nodes": ["WORK-0001"], "policy": POLICY}
        with self.assertRaises(PermissionError_):
            self.service.switch_policy(payload, operator_role="viewer")
        with self.assertRaises(PermissionError_):
            self.service.create_entry({"策略编号": "X", "节点标识": "Y", "下载带宽上限": "1"}, operator_role="operator")
        with self.assertRaises(PermissionError_):
            self.service.run_action(1, "启用策略", operator_role="auditor")
        with self.assertRaises(PermissionError_):
            self.service.writeback_traffic(
                {"node_id": "WORK-0001", "traffic_mb": 1, "duration_seconds": 1,
                 "snapshot_version": 1, "idempotency_key": "wb-perm"},
                operator_role="viewer",
            )
        # 只读角色可以查询
        self.assertEqual(len(self.service.node_states()), 3)
        self.assertEqual(len(self.service.landing_list()), 3)
        # 被拒后没有产生任何批次
        self.assertEqual(store.rows(BANDWIDTH_SWITCH_BATCH), [])

    # 14. 调度落点侧：读取新分配 + 回写流量（自动幂等键） ----------------------
    def test_dispatch_landing_integration(self) -> None:
        landing = self.dispatch.fetch_bandwidth_allocations()
        self.assertEqual(len(landing), 3)
        report = {"node_id": "WORK-0001", "traffic_mb": 200, "duration_seconds": 60, "snapshot_version": 9}
        first = self.dispatch.report_download_traffic(report, operator_role="admin")
        self.assertFalse(first["重复下发"])
        # 不带幂等键时自动生成确定性键，重复上报同快照只算一次
        second = self.dispatch.report_download_traffic(report, operator_role="admin")
        self.assertTrue(second["重复下发"])
        used = node_state("WORK-0001")["已用突发MB"]
        third = self.dispatch.report_download_traffic({**report, "snapshot_version": 10}, operator_role="admin")
        self.assertFalse(third["重复下发"])
        self.assertNotEqual(node_state("WORK-0001")["已用突发MB"], used)

    # 15. 审计可回查：批次里切换边界、配额版本与流量快照共同解释下一轮 ----------
    def test_batch_audit_trail(self) -> None:
        self.service.switch_policy(
            {"idempotency_key": "audit-1", "affected_nodes": ["WORK-0001", "WORK-0002"], "policy": POLICY},
            operator_role="admin", now=MOMENT,
        )
        batch = engine.list_batches()[-1]
        self.assertEqual(len(batch["切换边界"]), 2)
        self.assertEqual(len(batch["配额校验"]), 2)
        for boundary in batch["切换边界"]:
            self.assertIn("旧基础带宽Mbps", boundary)
            self.assertIn("新生效速率Mbps", boundary)
            self.assertIn("切换前流量快照版本", boundary)
        for check in batch["配额校验"]:
            self.assertGreaterEqual(check["剩余突发MB"], 0.0)
            self.assertIn("流量快照版本", check)
            self.assertIn("配额版本", check)
        fetched = engine.get_batch(batch["批次号"])
        self.assertEqual(fetched["批次号"], batch["批次号"])
        # 从分配数据可确认本次处理范围
        scoped = engine.list_allocations(batch_no=batch["批次号"])
        self.assertEqual({row["节点标识"] for row in scoped}, {"WORK-0001", "WORK-0002"})
        self.assertTrue(all(row["处理范围"] == "受影响节点" for row in scoped))

    # 15b. 完整闭环：切换 -> 落点读取 -> 流量回写 -> 下一轮切换由两者共同解释 ----
    def test_full_cycle_traffic_and_quota_explain_next_round(self) -> None:
        # 第一轮切换：WORK-0001 基础25/突发50/额度600
        self.service.switch_policy(
            {"idempotency_key": "cycle-1", "affected_nodes": ["WORK-0001"], "policy": POLICY},
            operator_role="admin", now=MOMENT,
        )
        landing_first = self.dispatch.fetch_bandwidth_allocations(node_id="WORK-0001")[0]
        self.assertEqual(landing_first["生效速率Mbps"], 50.0)

        # 调度落点侧回写流量：基础允许 25*60/8=187.5MB，下载 800MB -> 突发 612.5MB，额度封顶耗尽
        self.dispatch.report_download_traffic(
            {"node_id": "WORK-0001", "traffic_mb": 800, "duration_seconds": 60, "snapshot_version": 21},
            operator_role="admin",
        )
        state = node_state("WORK-0001")
        self.assertEqual(state["已用突发MB"], 600.0)
        self.assertEqual(state["流量快照版本"], 21)

        # 下一轮切换：流量快照（v21）与配额版本共同参与，耗尽状态被新批次解释
        result = self.service.switch_policy(
            {"idempotency_key": "cycle-2", "affected_nodes": ["WORK-0001"],
             "policy": {**POLICY, "突发额度MB": 800}},
            operator_role="admin", now=MOMENT,
        )
        batch = result["批次"]
        check = batch["配额校验"][0]
        self.assertEqual(check["流量快照版本"], 21)
        # 已用 600 被真实保留，新额度 800 下剩余 200
        self.assertEqual(check["已用突发MB"], 600.0)
        self.assertEqual(check["剩余突发MB"], 200.0)
        self.assertEqual(active_allocation("WORK-0001")["配额版本"], check["配额版本"] + 1)
        # 落点清单已同步到第二轮生效版本，且带回累计实际下载流量
        landing_next = self.dispatch.fetch_bandwidth_allocations(node_id="WORK-0001")[0]
        self.assertEqual(landing_next["分配版本号"], active_allocation("WORK-0001")["版本号"])
        self.assertEqual(landing_next["实际下载流量MB"], 3000.0 + 800.0)
        self.assertEqual(landing_next["流量快照版本"], 21)

    # 16. 非法入参被拦下并给出可读错误 ----------------------------------------
    def test_invalid_inputs(self) -> None:
        with self.assertRaises(ValueError):
            self.service.switch_policy(
                {"idempotency_key": "bad-1", "affected_nodes": [], "policy": POLICY},
                operator_role="admin",
            )
        with self.assertRaises(ValueError):
            self.service.switch_policy(
                {"idempotency_key": "bad-2", "affected_nodes": ["WORK-9999"], "policy": POLICY},
                operator_role="admin",
            )
        with self.assertRaises(ValueError):
            self.service.switch_policy(
                {"idempotency_key": "bad-3", "affected_nodes": ["WORK-0001"],
                 "policy": {**POLICY, "突发上限Mbps": 10}},  # 上限低于基础带宽
                operator_role="admin",
            )
        with self.assertRaises(ValueError):
            self.service.switch_policy(
                {"idempotency_key": "bad-4", "affected_nodes": ["WORK-0001"],
                 "policy": {**POLICY, "基础带宽Mbps": -5}},
                operator_role="admin",
            )
        with self.assertRaises(ValueError):
            self.service.switch_policy(
                {"affected_nodes": ["WORK-0001"], "policy": POLICY},  # 缺幂等键
                operator_role="admin",
            )
        with self.assertRaises(ValueError):
            self.service.writeback_traffic(
                {"node_id": "WORK-0001", "traffic_mb": -1, "duration_seconds": 10,
                 "snapshot_version": 1, "idempotency_key": "wb-bad"},
                operator_role="admin",
            )


if __name__ == "__main__":
    main(verbosity=2)
