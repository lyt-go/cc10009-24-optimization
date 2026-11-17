import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/dispatch'
const columns = [
  "策略编号",
  "目标域名",
  "分配算法",
  "权重因子",
  "节点亲和性",
  "负载阈值",
  "再平衡间隔",
  "溢出处理"
]
const actions = ["提交策略验证", "启用策略", "停用策略"]

type Row = Record<string, string | number | boolean | null>

interface LandingRow extends Record<string, unknown> {
  节点标识: string
  分配版本号: number
  基础带宽Mbps: number
  突发上限Mbps: number
  生效速率Mbps: number
  剩余突发MB: number
  实际下载流量MB: number
  同步状态: string
  流量快照版本: number
  配额版本: number
  回写时间: string | null
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export default function DispatchView() {
  const [rows, setRows] = useState<Row[]>([])
  const [landing, setLanding] = useState<LandingRow[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [infoMessage, setInfoMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])
  const [report, setReport] = useState({ node_id: '', traffic_mb: '100', duration_seconds: '60', snapshot_version: '1' })

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const [policyPage, landingPage] = await Promise.all([
        getJson<{ items: Row[]; total: number }>(`${ENDPOINT}?${query}`),
        getJson<{ items: LandingRow[] }>(`${ENDPOINT}/bandwidth/landing`),
      ])
      setRows(policyPage.items ?? [])
      setTotal(policyPage.total ?? 0)
      setLanding(landingPage.items ?? [])
      setReport((prev) => ({ ...prev, node_id: prev.node_id || landingPage.items[0]?.节点标识 || '' }))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分配列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { action } }),
      })
      if (!response.ok) {
        throw new Error('请求分配动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '请求分配操作失败')
    }
  }

  async function submitTrafficReport() {
    setErrorMessage('')
    setInfoMessage('')
    try {
      const response = await request(`${ENDPOINT}/bandwidth/traffic-report`, {
        method: 'POST',
        body: JSON.stringify({
          values: {
            node_id: report.node_id,
            traffic_mb: Number(report.traffic_mb),
            duration_seconds: Number(report.duration_seconds),
            snapshot_version: Number(report.snapshot_version),
          },
        }),
      })
      if (!response.ok) {
        const detail = await response.json().catch(() => ({}))
        throw new Error(detail.detail ?? '流量回写失败')
      }
      const body = await response.json()
      if (!body.ok) {
        throw new Error(body.message)
      }
      const entry = body.entry as Record<string, unknown>
      setInfoMessage(entry.重复下发 ? '该批流量已回写过，按幂等规则跳过，未重复计入配额' : '实际下载流量已回写，突发配额与生效速率已刷新')
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流量回写失败')
    }
  }

  return (
    <section className="page" data-module="dispatch">
      <header className="page-head">
        <div>
          <h2>请求分配管理（调度落点侧）</h2>
          <p className="page-desc">读取切换后的节点带宽分配按版本限速，并把实际下载流量回写给下一轮配额计算。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('分配策略登记入口尚未接入审批流')}>
            登记分配策略
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出请求分配清单
          </button>
        </div>
      </header>

      {errorMessage && <p className="error-text">{errorMessage}</p>}
      {infoMessage && <p className="ok-text">{infoMessage}</p>}

      <div className="section-block">
        <h3 className="section-title">调度落点侧带宽分配
          <span className="section-hint">每个节点当前生效的唯一分配版本，下载执行器据此限速</span>
        </h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>节点标识</th><th>分配版本</th><th>基础/突发上限Mbps</th><th>生效速率Mbps</th>
              <th>剩余突发MB</th><th>实际下载流量MB</th><th>快照/配额版本</th><th>同步状态</th><th>回写时间</th>
            </tr>
          </thead>
          <tbody>
            {landing.map((row) => (
              <tr key={row.节点标识}>
                <td>{row.节点标识}</td>
                <td>v{String(row.分配版本号)}</td>
                <td>{row.基础带宽Mbps} / {row.突发上限Mbps}</td>
                <td>{row.生效速率Mbps}</td>
                <td>{row.剩余突发MB}</td>
                <td>{row.实际下载流量MB}</td>
                <td>v{String(row.流量快照版本)} / v{String(row.配额版本)}</td>
                <td><span className={`badge ${row.同步状态 === '已回写' ? 'ok' : 'warn'}`}>{String(row.同步状态)}</span></td>
                <td>{row.回写时间 ?? '—'}</td>
              </tr>
            ))}
            {landing.length === 0 && <tr><td className="empty-state" colSpan={9}>落点清单尚未生成</td></tr>}
          </tbody>
        </table>
      </div>

      <div className="section-block">
        <h3 className="section-title">实际下载流量回写
          <span className="section-hint">同节点同快照版本重复上报只算一次（幂等键自动生成）</span>
        </h3>
        <div className="form-grid">
          <label>
            <span>节点标识</span>
            <select value={report.node_id} onChange={(event) => setReport((prev) => ({ ...prev, node_id: event.target.value }))}>
              {landing.map((row) => <option key={row.节点标识} value={row.节点标识}>{row.节点标识}</option>)}
            </select>
          </label>
          <label><span>本次下载流量MB</span><input type="number" min="0" value={report.traffic_mb}
            onChange={(event) => setReport((prev) => ({ ...prev, traffic_mb: event.target.value }))} /></label>
          <label><span>统计区间秒</span><input type="number" min="1" value={report.duration_seconds}
            onChange={(event) => setReport((prev) => ({ ...prev, duration_seconds: event.target.value }))} /></label>
          <label><span>流量快照版本</span><input type="number" min="1" value={report.snapshot_version}
            onChange={(event) => setReport((prev) => ({ ...prev, snapshot_version: event.target.value }))} /></label>
          <button className="btn primary" type="button" onClick={() => void submitTrafficReport()}>回写下载流量</button>
        </div>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无请求分配数据，可先登记分配策略</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条请求分配记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
