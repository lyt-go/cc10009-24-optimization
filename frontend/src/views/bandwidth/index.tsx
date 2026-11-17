import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/bandwidth'
const columns = [
  "策略编号",
  "节点标识",
  "下载带宽上限",
  "上传带宽上限",
  "突发额度",
  "限速粒度秒",
  "优先级权重",
  "生效时段"
]
const actions = ["提交策略评审", "启用策略", "暂停策略"]

type Row = Record<string, string | number | boolean | null>
type JsonRow = Record<string, unknown>
type Role = 'admin' | 'viewer'

interface NodeState extends JsonRow {
  节点标识: string
  在线: boolean
  基础带宽Mbps: number
  突发上限Mbps: number
  突发额度MB: number
  已用突发MB: number
  累计下载流量MB: number
  流量快照版本: number
  配额版本: number
  策略到期时间: string | null
}

interface Allocation extends JsonRow {
  id: number
  节点标识: string
  切换批次号: string
  版本号: number
  生效速率Mbps: number
  剩余突发MB: number
  流量快照版本: number
  配额版本: number
  状态: string
  冲突标记: boolean
  处理范围: string
}

interface Batch extends JsonRow {
  批次号: string
  操作时间: string
  切换依据: string
  受影响节点: string[]
  未受影响节点: string[]
  突发耗尽节点: string[]
  安全边界内截断节点: string[]
  冲突记录: Array<Record<string, unknown>>
  配额校验: Array<Record<string, unknown>>
  切换边界: Array<Record<string, unknown>>
}

interface LandingRow extends JsonRow {
  节点标识: string
  分配版本号: number
  生效速率Mbps: number
  剩余突发MB: number
  实际下载流量MB: number
  同步状态: string
  流量快照版本: number
  配额版本: number
}

function newIdempotencyKey() {
  return `ui-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export default function BandwidthView() {
  const [role, setRole] = useState<Role>('admin')
  const [rows, setRows] = useState<Row[]>([])
  const [nodes, setNodes] = useState<NodeState[]>([])
  const [allocations, setAllocations] = useState<Allocation[]>([])
  const [batches, setBatches] = useState<Batch[]>([])
  const [landing, setLanding] = useState<LandingRow[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [infoMessage, setInfoMessage] = useState('')
  const [selectedBatch, setSelectedBatch] = useState<Batch | null>(null)

  const [pickedNodes, setPickedNodes] = useState<string[]>([])
  const [policy, setPolicy] = useState({ 基础带宽Mbps: '25', 突发上限Mbps: '50', 突发额度MB: '600', 限速粒度秒: '1', 优先级权重: '10' })
  const [reason, setReason] = useState('')
  const [idempotencyKey, setIdempotencyKey] = useState(() => newIdempotencyKey())
  const [submitting, setSubmitting] = useState(false)

  const isAdmin = role === 'admin'
  const headers = useMemo(() => ({ 'X-User-Role': role }), [role])

  const reload = useCallback(async () => {
    setErrorMessage('')
    try {
      const [policyPage, nodePage, allocationPage, batchPage, landingPage] = await Promise.all([
        getJson<{ items: Row[]; total: number }>(`${ENDPOINT}?page=1&size=200`),
        getJson<{ items: NodeState[] }>(`${ENDPOINT}/runtime/nodes`),
        getJson<{ items: Allocation[] }>(`${ENDPOINT}/runtime/allocations`),
        getJson<{ items: Batch[] }>(`${ENDPOINT}/runtime/batches`),
        getJson<{ items: LandingRow[] }>(`${ENDPOINT}/runtime/landing`),
      ])
      setRows(policyPage.items)
      setTotal(policyPage.total)
      setNodes(nodePage.items)
      setAllocations(allocationPage.items)
      setBatches(batchPage.items)
      setLanding(landingPage.items)
      setPickedNodes((prev) => (prev.length ? prev : nodePage.items.map((item) => item.节点标识)))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽运行态读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
        body: JSON.stringify({ values: { action } }),
      })
      if (response.status === 403) {
        const detail = await response.json()
        throw new Error(detail.detail ?? '当前角色只读，无法执行策略动作')
      }
      if (!response.ok) {
        throw new Error('带宽控制动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '带宽控制操作失败')
    }
  }

  function toggleNode(nodeId: string) {
    setPickedNodes((prev) => (prev.includes(nodeId) ? prev.filter((item) => item !== nodeId) : [...prev, nodeId]))
  }

  async function submitSwitch() {
    setErrorMessage('')
    setInfoMessage('')
    if (!pickedNodes.length) {
      setErrorMessage('请至少选择一个受影响节点')
      return
    }
    setSubmitting(true)
    try {
      const response = await request(`${ENDPOINT}/runtime/switch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
        body: JSON.stringify({
          values: {
            idempotency_key: idempotencyKey,
            affected_nodes: pickedNodes,
            policy: {
              基础带宽Mbps: Number(policy.基础带宽Mbps),
              突发上限Mbps: Number(policy.突发上限Mbps),
              突发额度MB: Number(policy.突发额度MB),
              限速粒度秒: Number(policy.限速粒度秒),
              优先级权重: Number(policy.优先级权重),
            },
            reason: reason || '前端触发增量限速切换',
          },
        }),
      })
      if (response.status === 403) {
        const detail = await response.json()
        throw new Error(detail.detail ?? '当前角色只读，无法切换带宽策略')
      }
      const body = await response.json()
      if (!body.ok) {
        throw new Error(body.message)
      }
      setInfoMessage(body.message)
      setIdempotencyKey(newIdempotencyKey())
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量切换失败')
    } finally {
      setSubmitting(false)
    }
  }

  function replaySameKey() {
    // 故意保留当前幂等键再点一次，用于演示重复下发不会产生双重限速
    setInfoMessage('已保留同一幂等键，再次提交将命中幂等去重')
  }

  const exhaustedCount = nodes.filter((node) => Number(node.已用突发MB) >= Number(node.突发额度MB)).length
  const offlineCount = nodes.filter((node) => !node.在线).length

  return (
    <section className="page" data-module="bandwidth">
      <header className="page-head">
        <div>
          <h2>带宽控制管理</h2>
          <p className="page-desc">增量限速切换只重算受影响节点，配额、版本与调度落点清单全程可核对。</p>
        </div>
        <div className="page-actions">
          <label className="role-switch">
            当前角色
            <select value={role} onChange={(event) => setRole(event.target.value as Role)}>
              <option value="admin">管理员（可写）</option>
              <option value="viewer">只读角色（viewer）</option>
            </select>
          </label>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出带宽控制清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card"><span className="stat-label">纳管节点</span><strong className="stat-value">{nodes.length}</strong></article>
        <article className="stat-card"><span className="stat-label">突发耗尽节点</span><strong className="stat-value">{exhaustedCount}</strong></article>
        <article className="stat-card"><span className="stat-label">离线节点</span><strong className="stat-value">{offlineCount}</strong></article>
        <article className="stat-card"><span className="stat-label">切换批次</span><strong className="stat-value">{batches.length}</strong></article>
      </div>

      {!isAdmin && <p className="badge warn">只读模式：节点归属与限速参数仅管理员可改，写操作将被后端拒绝（403）</p>}
      {errorMessage && <p className="error-text">{errorMessage}</p>}
      {infoMessage && <p className="ok-text">{infoMessage}</p>}

      <div className="section-block">
        <h3 className="section-title">增量限速切换
          <span className="section-hint">读取受影响节点、突发额度与当前下载流量，重算切换边界并校验剩余配额</span>
        </h3>
        <div className="node-pick">
          {nodes.map((node) => (
            <label key={node.节点标识}>
              <input type="checkbox" checked={pickedNodes.includes(node.节点标识)} onChange={() => toggleNode(node.节点标识)} disabled={!isAdmin} />
              {node.节点标识}
              <span className={`badge ${node.在线 ? 'ok' : 'off'}`}>{node.在线 ? '在线' : '离线'}</span>
            </label>
          ))}
        </div>
        <div className="form-grid">
          {Object.entries(policy).map(([field, value]) => (
            <label key={field}>
              <span>{field}</span>
              <input value={value} disabled={!isAdmin}
                onChange={(event) => setPolicy((prev) => ({ ...prev, [field]: event.target.value }))} />
            </label>
          ))}
          <label>
            <span>切换依据</span>
            <input value={reason} placeholder="如：晚高峰前提速" disabled={!isAdmin}
              onChange={(event) => setReason(event.target.value)} />
          </label>
          <label>
            <span>幂等键（重复下发只生效一次）</span>
            <input value={idempotencyKey} readOnly style={{ minWidth: 240 }} />
          </label>
          <button className="btn primary" type="button" disabled={!isAdmin || submitting} onClick={() => void submitSwitch()}>
            {submitting ? '切换中…' : '执行增量切换'}
          </button>
          <button className="btn ghost" type="button" disabled={!isAdmin} onClick={replaySameKey}>
            重复下发演示
          </button>
        </div>
      </div>

      <div className="section-block">
        <h3 className="section-title">节点带宽运行态
          <span className="section-hint">突发额度耗尽后回落基础带宽，离线或过期不突破安全边界，剩余额度不为负</span>
        </h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>节点标识</th><th>在线</th><th>基础带宽Mbps</th><th>突发上限Mbps</th>
              <th>突发额度MB</th><th>已用突发MB</th><th>累计下载流量MB</th>
              <th>流量快照版本</th><th>配额版本</th><th>到期时间</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((node) => {
              const exhausted = Number(node.已用突发MB) >= Number(node.突发额度MB)
              return (
                <tr key={node.节点标识}>
                  <td>{node.节点标识}</td>
                  <td><span className={`badge ${node.在线 ? 'ok' : 'off'}`}>{node.在线 ? '在线' : '离线'}</span></td>
                  <td>{node.基础带宽Mbps}</td>
                  <td>{node.突发上限Mbps}</td>
                  <td>{node.突发额度MB}</td>
                  <td><span className={`badge ${exhausted ? 'danger' : 'ok'}`}>{node.已用突发MB}</span></td>
                  <td>{node.累计下载流量MB}</td>
                  <td>v{node.流量快照版本}</td>
                  <td>v{node.配额版本}</td>
                  <td>{node.策略到期时间 ?? '—'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      <div className="section-block">
        <h3 className="section-title">节点带宽分配（调度落点侧清单）
          <span className="section-hint">每个节点只有一个生效版本，结果记录已同步到调度落点业务面</span>
        </h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>节点标识</th><th>分配版本</th><th>生效速率Mbps</th><th>剩余突发MB</th>
              <th>实际下载流量MB</th><th>快照/配额版本</th><th>同步状态</th>
            </tr>
          </thead>
          <tbody>
            {landing.map((row) => (
              <tr key={row.节点标识}>
                <td>{row.节点标识}</td>
                <td>v{String(row.分配版本号)}</td>
                <td>{row.生效速率Mbps}</td>
                <td>{row.剩余突发MB}</td>
                <td>{row.实际下载流量MB}</td>
                <td>v{String(row.流量快照版本)} / v{String(row.配额版本)}</td>
                <td><span className={`badge ${row.同步状态 === '已回写' ? 'ok' : 'warn'}`}>{String(row.同步状态)}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="section-block">
        <h3 className="section-title">切换批次审计
          <span className="section-hint">切换依据、处理范围、配额校验与冲突双版本均可回查</span>
        </h3>
        <table className="data-table">
          <thead>
            <tr><th>批次号</th><th>操作时间</th><th>切换依据</th><th>受影响节点</th><th>未受影响节点</th><th>耗尽/截断</th><th>冲突</th><th></th></tr>
          </thead>
          <tbody>
            {batches.map((batch) => (
              <tr key={batch.批次号}>
                <td>{batch.批次号}</td>
                <td>{batch.操作时间}</td>
                <td>{batch.切换依据}</td>
                <td>{batch.受影响节点.join('、')}</td>
                <td>{batch.未受影响节点.join('、') || '—'}</td>
                <td>
                  {batch.突发耗尽节点.length > 0 && <span className="badge danger">耗尽 {batch.突发耗尽节点.length}</span>}
                  {batch.安全边界内截断节点.length > 0 && <span className="badge warn">截断 {batch.安全边界内截断节点.length}</span>}
                  {batch.突发耗尽节点.length === 0 && batch.安全边界内截断节点.length === 0 && '—'}
                </td>
                <td>{batch.冲突记录.length > 0 ? <span className="badge warn">{batch.冲突记录.length} 组双版本</span> : '—'}</td>
                <td className="row-actions">
                  <button className="link" type="button" onClick={() => setSelectedBatch(batch)}>查看核对明细</button>
                </td>
              </tr>
            ))}
            {batches.length === 0 && (
              <tr><td className="empty-state" colSpan={8}>尚无切换批次，执行一次增量切换后这里会留下可核对记录</td></tr>
            )}
          </tbody>
        </table>
        {selectedBatch && (
          <div className="detail-grid">
            <div className="detail-card">
              <dl>
                <dt>配额校验</dt><dd>{JSON.stringify(selectedBatch.配额校验, null, 0)}</dd>
                <dt>切换边界</dt><dd>{JSON.stringify(selectedBatch.切换边界, null, 0)}</dd>
                {selectedBatch.冲突记录.length > 0 && (<><dt>冲突记录</dt><dd>{JSON.stringify(selectedBatch.冲突记录, null, 0)}</dd></>)}
              </dl>
              <button className="link" type="button" onClick={() => setSelectedBatch(null)}>收起明细</button>
            </div>
          </div>
        )}
      </div>

      <div className="section-block">
        <h3 className="section-title">分配版本流水
          <span className="section-hint">同一份分配反复处理只产生一条生效版本；冲突时同节点保留截止版与生效版两条</span>
        </h3>
        <table className="data-table">
          <thead>
            <tr><th>节点</th><th>批次</th><th>版本</th><th>生效速率Mbps</th><th>剩余突发MB</th><th>快照/配额</th><th>处理范围</th><th>状态</th></tr>
          </thead>
          <tbody>
            {allocations.map((item) => (
              <tr key={item.id}>
                <td>{item.节点标识}</td>
                <td>{item.切换批次号}</td>
                <td>v{item.版本号}</td>
                <td>{item.生效速率Mbps}</td>
                <td>{item.剩余突发MB}</td>
                <td>v{String(item.流量快照版本)} / v{String(item.配额版本)}</td>
                <td>{item.处理范围}{item.冲突标记 ? <span className="badge warn">冲突</span> : null}</td>
                <td>{item.状态}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="section-block">
        <h3 className="section-title">带宽策略清单（原策略台账）</h3>
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((column) => <th key={column}>{column}</th>)}
              <th>状态</th><th>可执行动作</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={String(row.id)}>
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td>{String(row.status ?? '—')}</td>
                <td className="row-actions">
                  {actions.map((action) => (
                    <button key={action} className="link" type="button" disabled={!isAdmin}
                      onClick={() => void runAction(action, row)}>
                      {action}
                    </button>
                  ))}
                </td>
              </tr>
            ))}
            {rows.length === 0 && <tr><td className="empty-state" colSpan={columns.length + 2}>暂无带宽策略数据</td></tr>}
          </tbody>
        </table>
      </div>

      <footer className="page-foot">
        <span>共 {total} 条带宽策略记录 · 角色：{isAdmin ? '管理员可写' : '只读'}</span>
      </footer>
    </section>
  )
}
