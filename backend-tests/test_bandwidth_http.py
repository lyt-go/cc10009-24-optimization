"""带宽增量切换的 HTTP 调用链测试：路由 -> service -> engine -> store。

用 FastAPI TestClient 验证角色头权限（403/只读放行）、运行态接口、切换与流量
回写的端到端报文，以及并发请求下的结果一致性。
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import TestCase, main

sys.path.insert(0, "/workspace/backend")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app.services.bandwidth_engine import engine  # noqa: E402
from app.store import BANDWIDTH_RUNTIME_TABLES, store  # noqa: E402

POLICY = {"基础带宽Mbps": 25, "突发上限Mbps": 50, "突发额度MB": 600,
          "限速粒度秒": 1, "优先级权重": 10}


def reset_runtime() -> None:
    for table in BANDWIDTH_RUNTIME_TABLES:
        store.rows(table).clear()
    engine.ensure_bootstrapped()


class BandwidthHttpTests(TestCase):
    def setUp(self) -> None:
        reset_runtime()
        self.client = TestClient(app)

    def test_runtime_read_endpoints(self) -> None:
        for path in (
            "/api/bandwidth/runtime/nodes",
            "/api/bandwidth/runtime/allocations",
            "/api/bandwidth/runtime/landing",
            "/api/dispatch/bandwidth/landing",
        ):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200, path)
            self.assertEqual(response.json()["total"], 3)
        # 引导阶段只有初始分配，不产生切换批次
        self.assertEqual(self.client.get("/api/bandwidth/runtime/batches").json()["total"], 0)

    def test_switch_requires_admin_role(self) -> None:
        payload = {"values": {"idempotency_key": "http-perm",
                              "affected_nodes": ["WORK-0001"], "policy": POLICY}}
        denied = self.client.post("/api/bandwidth/runtime/switch", json=payload,
                                  headers={"X-User-Role": "viewer"})
        self.assertEqual(denied.status_code, 403)
        self.assertIn("只读权限", denied.json()["detail"])
        # 被拒后没有产生批次
        self.assertEqual(self.client.get("/api/bandwidth/runtime/batches").json()["total"], 0)

        allowed = self.client.post("/api/bandwidth/runtime/switch", json=payload,
                                   headers={"X-User-Role": "admin"})
        self.assertEqual(allowed.status_code, 200)
        self.assertTrue(allowed.json()["ok"])

    def test_switch_and_query_scope_end_to_end(self) -> None:
        payload = {"values": {"idempotency_key": "http-1",
                              "affected_nodes": ["WORK-0001", "WORK-0002"],
                              "policy": POLICY, "reason": "高峰前提速"}}
        response = self.client.post("/api/bandwidth/runtime/switch", json=payload)
        self.assertEqual(response.status_code, 200)
        batch = response.json()["entry"]
        self.assertEqual(batch["受影响节点"], ["WORK-0001", "WORK-0002"])
        self.assertEqual(batch["未受影响节点"], ["WORK-0003"])

        batch_no = batch["批次号"]
        scoped = self.client.get(f"/api/bandwidth/runtime/allocations?batch_no={batch_no}").json()
        self.assertEqual({item["节点标识"] for item in scoped["items"]},
                         {"WORK-0001", "WORK-0002"})
        detail = self.client.get(f"/api/bandwidth/runtime/batches/{batch_no}")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["切换依据"], "高峰前提速")

    def test_duplicate_switch_is_idempotent_over_http(self) -> None:
        payload = {"values": {"idempotency_key": "http-dup",
                              "affected_nodes": ["WORK-0001"], "policy": POLICY}}
        first = self.client.post("/api/bandwidth/runtime/switch", json=payload).json()
        second = self.client.post("/api/bandwidth/runtime/switch", json=payload).json()
        self.assertEqual(first["entry"]["批次号"], second["entry"]["批次号"])
        self.assertIn("幂等", second["message"])
        batches = self.client.get("/api/bandwidth/runtime/batches").json()["total"]
        self.assertEqual(batches, 1)

    def test_traffic_writeback_via_dispatch_and_bandwidth(self) -> None:
        report = {"values": {"node_id": "WORK-0001", "traffic_mb": 240,
                             "duration_seconds": 60, "snapshot_version": 11}}
        first = self.client.post("/api/dispatch/bandwidth/traffic-report", json=report)
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json()["ok"])
        entry = first.json()["entry"]
        # 基础允许量 150MB，突发消耗 90MB，已用 120+90=210MB
        self.assertEqual(entry["已用突发MB"], 210.0)
        self.assertEqual(entry["剩余突发MB"], 290.0)

        # 同快照重复上报只算一次（dispatch 侧自动补确定性幂等键）
        second = self.client.post("/api/dispatch/bandwidth/traffic-report", json=report).json()
        self.assertTrue(second["entry"]["重复下发"])

        landing = self.client.get("/api/dispatch/bandwidth/landing?node_id=WORK-0001").json()
        row = landing["items"][0]
        self.assertEqual(row["同步状态"], "已回写")
        self.assertEqual(row["流量快照版本"], 11)

        # 普通角色无法伪造流量
        denied = self.client.post(
            "/api/bandwidth/runtime/traffic-writeback",
            json={"values": {**report["values"], "idempotency_key": "x"}},
            headers={"X-User-Role": "operator"},
        )
        self.assertEqual(denied.status_code, 403)

    def test_concurrent_requests_converge(self) -> None:
        barrier = threading.Barrier(8)
        payload = {"values": {"idempotency_key": "http-race",
                              "affected_nodes": ["WORK-0001"], "policy": POLICY}}

        def fire(_: int) -> int:
            barrier.wait()
            return self.client.post("/api/bandwidth/runtime/switch", json=payload).status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(fire, range(8)))
        self.assertEqual(set(statuses), {200})
        self.assertEqual(self.client.get("/api/bandwidth/runtime/batches").json()["total"], 1)
        landing = self.client.get("/api/bandwidth/runtime/landing?node_id=WORK-0001").json()
        self.assertEqual(landing["total"], 1)

    def test_invalid_payload_returns_ok_false(self) -> None:
        response = self.client.post(
            "/api/bandwidth/runtime/switch",
            json={"values": {"idempotency_key": "bad", "affected_nodes": ["WORK-0001"],
                             "policy": {**POLICY, "突发上限Mbps": 1}}},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertIn("突发上限", body["message"])

    def test_unknown_batch_404(self) -> None:
        response = self.client.get("/api/bandwidth/runtime/batches/SW-NOPE-9999")
        self.assertEqual(response.status_code, 404)


if __name__ == "__main__":
    main(verbosity=2)
