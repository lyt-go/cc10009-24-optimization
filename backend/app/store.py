"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

带宽增量切换相关的运行态表（节点带宽状态、分配快照、切换批次、调度落点清单、
权限幂等记录）也收在这里，并由一把可重入锁保护：带宽控制并发时所有读-改-写
在同一把锁内完成，结果不会分叉。
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Iterator

from app.seed import SEED_ROWS

# 带宽运行态表名集中放这里，service/engine 与 router 共用，避免字符串散落。
NODE_BANDWIDTH_STATE = "node_bandwidth_state"
BANDWIDTH_ALLOCATION = "bandwidth_allocation"
BANDWIDTH_SWITCH_BATCH = "bandwidth_switch_batch"
DISPATCH_BANDWIDTH_LIST = "dispatch_bandwidth_list"
BANDWIDTH_IDEMPOTENCY = "bandwidth_idempotency"

BANDWIDTH_RUNTIME_TABLES = (
    NODE_BANDWIDTH_STATE,
    BANDWIDTH_ALLOCATION,
    BANDWIDTH_SWITCH_BATCH,
    DISPATCH_BANDWIDTH_LIST,
    BANDWIDTH_IDEMPOTENCY,
)


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        for table in BANDWIDTH_RUNTIME_TABLES:
            self._tables.setdefault(table, [])
        # 可重入锁：同一调用链内层函数再次进入不会自锁，并发切换被整体串行化。
        self._lock = threading.RLock()

    @contextmanager
    def locked(self) -> Iterator[None]:
        """串行化带宽控制的读-改-写临界区，保证并发结果不分叉。"""
        with self._lock:
            yield

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        """在临界区内取一张表的下一个自增 id。"""
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
