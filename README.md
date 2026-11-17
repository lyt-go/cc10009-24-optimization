# 分布式网页抓取调度平台

面向分布式网页抓取任务调度、节点协同、请求分配、反爬策略编排、数据校验入库与抓取质量大盘的采集工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点 | `worker` | 工作节点 | 节点编号、主机地址、并发槽位数 |
| 请求分配 | `dispatch` | 分配策略 | 策略编号、目标域名、分配算法 |
| 抓取作业 | `crawljob` | 作业定义 | 作业编号、目标站点、起始URL |
| URL过滤 | `urlfilter` | 过滤规则 | 规则编号、包含模式、排除模式 |
| DNS解析 | `dnsresolver` | 解析配置 | 配置编号、域名模式、DNS服务器 |
| 内容提取 | `contentextract` | 提取器 | 提取器编号、内容类型、正文选择器 |
| 存储策略 | `storagestrategy` | 存储方案 | 方案编号、存储类型、连接参数 |
| 渲染任务 | `rendertask` | 渲染作业 | 作业编号、目标URL、浏览器版本 |
| 采集质量 | `qualityreport` | 质量报告 | 报告编号、作业编号、总URL数 |
| 带宽控制 | `bandwidth` | 带宽策略 | 策略编号、节点标识、下载带宽上限 |
| 重试队列 | `retryqueue` | 重试条目 | 条目编号、失败URL、失败原因码 || 翻页管理 | `pagemarker` | 翻页规则 | 规则编号、目标站点、翻页类型 |
| 增量标记 | `incrementmark` | 增量水位 | 水位编号、目标源、增量字段名 |
| 配置同步 | `configsync` | 同步任务 | 任务编号、源节点、目标节点 |
| 采集API | `scrapeapi` | API定义 | API编号、API名称、请求路径 |
| 数据抽样 | `datasample` | 抽样方案 | 方案编号、数据源、抽样方式 |
| 采集告警 | `alertpolicy` | 告警策略 | 策略编号、监控指标、触发条件 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA

## 带宽增量限速切换

带宽策略切换不再全量重算节点，而是按「受影响节点」做增量切换，调用链为
`router（取参/角色）→ services/bandwidth.py（权限与入参校验）→
services/bandwidth_engine.py（增量切换领域计算）→ store.py（带锁运行态表）`，
调度落点侧通过 `dispatch` 模块读取分配并回写下载流量。

### 关键约束的落点

| 约束 | 实现位置 |
| --- | --- |
| 只重算受影响节点，未受影响节点保持原限速与原版本 | `BandwidthSwitchEngine.switch_policy` 的 `处理范围` |
| 切换边界、剩余配额可核对 | 切换批次的 `切换边界`、`配额校验` 字段 |
| 突发耗尽后回落基础带宽，不同时突破基础带宽与突发上限 | `_effective_rate`，离线/过期同样截断 |
| 节点离线或策略过期不留负数余量 | 剩余额度 `max(0, 额度-已用)`，已用量截断到额度 |
| 重复下发/重复回写只算一次，不产生双重限速 | 幂等键表 `bandwidth_idempotency` |
| 同一份分配反复处理只有一个生效版本 | 旧版本置为「已被新版本替换」 |
| 带宽控制并发结果不分叉 | `store.locked()` 可重入锁串行化读-改-写 |
| 策略到期与任务完成同时刻的双版本 | 同节点落「冲突截止版 + 生效版」，绑定各自流量快照 |
| 实际流量与配额版本共同解释下一轮分配 | 流量回写刷新 `流量快照版本`，与 `配额版本` 成对留痕 |
| 节点归属与限速参数仅管理员可改 | 写接口校验 `X-User-Role`，非 admin 返回 403 |
| 分配同步到调度落点侧清单并更新业务面 | `dispatch_bandwidth_list`，每节点唯一生效版本 |

### 运行态接口

- `GET  /api/bandwidth/runtime/nodes` 节点带宽运行态（额度、已用量、版本）
- `GET  /api/bandwidth/runtime/allocations` 节点带宽分配快照（可按节点/批次过滤）
- `GET  /api/bandwidth/runtime/batches` 与 `/batches/{批次号}` 切换审计
- `GET  /api/bandwidth/runtime/landing` 调度落点侧清单
- `POST /api/bandwidth/runtime/switch` 增量切换（body 带 `idempotency_key`）
- `POST /api/bandwidth/runtime/traffic-writeback` 流量回写（管理员口径）
- `GET  /api/dispatch/bandwidth/landing` 调度落点侧读取当前生效分配
- `POST /api/dispatch/bandwidth/traffic-report` 调度落点侧回写实际下载流量

写接口通过请求头 `X-User-Role` 区分角色：`admin` 可写，`viewer`/`operator`/
`auditor` 只读并返回 403。后端测试位于独立目录 `backend-tests/`，用
`python -I` 运行，与应用代码隔离。

