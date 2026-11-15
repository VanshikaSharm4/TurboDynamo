import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Database, ArrowUpRight, Activity, Terminal, RotateCcw } from 'lucide-react'
import { ResponsiveContainer, AreaChart, Area, CartesianGrid, XAxis, YAxis, Tooltip } from 'recharts'
import { ClusterStatus } from './ClusterStatus'
import { HashRing } from '../../lib/hashRing'

const initialEntries: Record<string, string> = {
  'user:1001': '{"name":"Alice","plan":"Pro"}',
  'user:1002': '{"name":"Bob","plan":"Free"}',
  'session:8f2a': '{"user":"1001","active":true}',
  'product:42': '{"name":"Mechanical keyboard","stock":24}',
  'config:theme': 'light',
}
const history = Array.from({ length: 18 }, (_, i) => ({ time: `${String(10 + Math.floor(i / 6)).padStart(2, '0')}:${String((i % 6) * 10).padStart(2, '0')}`, reads: 1200 + Math.round(Math.sin(i * .8) * 250) + i * 35, writes: 600 + Math.round(Math.cos(i * .7) * 120) + i * 15 }))
const ring = new HashRing(150)
;['node-1', 'node-2', 'node-3'].forEach(n => ring.addNode(n))

function loadEntries() {
  try {
    const saved = JSON.parse(localStorage.getItem('turbodynamo-demo-v1') || 'null')
    if (saved && typeof saved === 'object' && !Array.isArray(saved) && Object.values(saved).every(v => typeof v === 'string')) return saved as Record<string, string>
  } catch { /* Storage may be unavailable; the demo still works in memory. */ }
  return { ...initialEntries }
}

export function DemoDashboard({ section, onNavigate }: { section: string; onNavigate: (section: string) => void }) {
  const [entries, setEntries] = useState(loadEntries)
  const [operation, setOperation] = useState('GET')
  const [key, setKey] = useState('user:1001')
  const [value, setValue] = useState(initialEntries['user:1001'])
  const [result, setResult] = useState('Select a sample key or execute GET to explore the response.')
  const [events, setEvents] = useState(['Cluster initialized · 3 nodes · preview', 'Replication completed · 3 copies · preview', 'Compaction completed · L0 to L1 · preview'])
  const route = key ? ring.routeKey(key, 3) : null
  const persist = (next: Record<string, string>) => {
    setEntries(next)
    try { localStorage.setItem('turbodynamo-demo-v1', JSON.stringify(next)) } catch { /* In-memory fallback. */ }
  }
  function execute() {
    if (!key.trim() && !['KEYS', 'SCAN'].includes(operation)) { setResult('Enter a key first.'); return }
    const next = { ...entries }
    if (operation === 'PUT') { Object.defineProperty(next, key, { value, enumerable: true, configurable: true, writable: true }); persist(next); setResult(JSON.stringify({ success: true, key, value, mode: 'browser demo' }, null, 2)) }
    if (operation === 'GET') setResult(Object.hasOwn(entries, key) ? JSON.stringify({ key, value: entries[key], mode: 'browser demo' }, null, 2) : `Key not found: ${key}`)
    if (operation === 'DELETE') { const exists = Object.hasOwn(next, key); delete next[key]; persist(next); setResult(exists ? `Deleted ${key} from this browser demo.` : `Key not found: ${key}`) }
    if (operation === 'KEYS') setResult(JSON.stringify(Object.keys(entries), null, 2))
    if (operation === 'SCAN') setResult(JSON.stringify(Object.fromEntries(Object.entries(entries).filter(([k]) => k.startsWith(key))), null, 2))
    setEvents(prev => [`${operation} ${operation === 'KEYS' ? 'all keys' : key} · browser demo`, ...prev].slice(0, 6))
  }
  const chart = <ResponsiveContainer width="100%" height={220}><AreaChart data={history}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#ededed" /><XAxis dataKey="time" tick={{ fontSize: 11, fill: '#737373' }} axisLine={false} tickLine={false} minTickGap={40} /><YAxis tick={{ fontSize: 11, fill: '#737373' }} axisLine={false} tickLine={false} width={45} /><Tooltip /><Area name="Reads / sec" type="monotone" dataKey="reads" stroke="#626262" fill="#e4e4e4" strokeWidth={2} /><Area name="Writes / sec" type="monotone" dataKey="writes" stroke="#aaaaaa" fill="#f4f4f4" strokeWidth={2} /></AreaChart></ResponsiveContainer>
  const cluster = <section className="demo-card"><ClusterStatus apiBase="" preview /><p className="demo-insight">Explore how real-world cluster data will appear: follow a key around the hash ring, inspect its replicas, and compare virtual-node distribution.</p></section>
  const setup = <section className="demo-card demo-setup"><div><span className="demo-eyebrow">TAKE IT FOR A REAL SPIN</span><h2>Run the actual database locally.</h2><p>This demo lives in your browser. Run the Go server to try real disk storage, API requests, and live metrics.</p><Link className="demo-link" to="/docs#installation">Read local setup documentation <ArrowUpRight size={15} /></Link><a className="demo-link" href="https://github.com/VanshikaSharm4/TurboDynamo#run-locally-on-windows" target="_blank" rel="noopener noreferrer">Repository & Windows instructions <ArrowUpRight size={15} /></a></div><div><span className="demo-eyebrow">POWERSHELL · GO 1.25+ / NODE.JS 22+</span><pre>{`git clone https://github.com/VanshikaSharm4/TurboDynamo.git\ncd TurboDynamo\ngo build -o bin/turbodynamo-server.exe ./cmd/server\n.\\bin\\turbodynamo-server.exe\n\n# In another terminal\ncd web\nnpm ci\nnpm run dev`}</pre><p className="demo-note">Open localhost:3001/dashboard, then choose Live backend.</p></div></section>
  return <div className="demo-dashboard">
    <header className="demo-heading"><div><span className="demo-eyebrow">TURBODYNAMO / INTERACTIVE SHOWCASE</span><h1>{({ overview: 'A small database. A bigger picture.', operations: 'Try the key-value playground.', cluster: 'Built to work together.', metrics: 'See the engine at work.', observability: 'Understand every moving part.', settings: 'Know what powers the system.' } as Record<string, string>)[section]}</h1><p>Explore distributed storage, follow a key, and try the workflow.</p></div><span className="demo-badge">Demo mode</span></header>
    <div className="demo-disclosure"><Database size={18} /><p><strong>A preview of your real-world dashboard.</strong> This is how real-world data will appear when your backend is connected. In Demo mode, you can explore the interface and try key-value operations in your browser.</p></div>
    {['overview', 'metrics', 'observability'].includes(section) && <div className="demo-stats">{[{ label: 'Browser keys', value: Object.keys(entries).length, hint: 'Real count in this demo' }, { label: 'Cluster nodes', value: '3', hint: 'Cluster preview' }, { label: 'Read latency', value: '2.4 ms', hint: 'Latency preview' }, { label: 'Replication', value: '3 copies', hint: 'Replication preview' }].map(s => <div className="demo-stat" key={s.label}><span>{s.label}</span><strong>{s.value}</strong><small>{s.hint}</small></div>)}</div>}
    {['overview', 'metrics'].includes(section) && <div className="demo-chart-grid"><section className="demo-card"><div className="demo-card-heading"><h2>Requests over time</h2><span className="demo-badge">Dashboard preview</span></div><p className="demo-note">Reads and writes per second · workload preview</p>{chart}</section><section className="demo-card"><span className="demo-eyebrow">WHY IT MATTERS</span><h2>Fast writes, organized storage.</h2><div className="demo-pipeline"><span>Write-ahead log</span><span>MemTable</span><span>Sorted disk files</span></div><p className="demo-insight">The real engine records writes in a log, holds recent entries in memory, then merges sorted files in the background.</p><button className="demo-link" onClick={() => { setOperation('PUT'); onNavigate('operations') }}>Try a write <ArrowUpRight size={15} /></button></section></div>}
    {['overview', 'operations'].includes(section) && <section className="demo-card"><div className="demo-card-heading"><h2>Key-value playground</h2><span className="demo-badge">Interactive · browser only</span></div><p className="demo-note">Start with user:1001. Update it, read it back, or create your own key.</p><div className="demo-playground"><div><div className="demo-tabs">{['GET', 'PUT', 'DELETE', 'SCAN', 'KEYS'].map(op => <button key={op} aria-pressed={operation === op} className={operation === op ? 'selected' : ''} onClick={() => setOperation(op)}>{op}</button>)}</div>{operation !== 'KEYS' && <label className="demo-field">{operation === 'SCAN' ? 'Key prefix' : 'Key'}<input value={key} onChange={e => setKey(e.target.value)} placeholder="user:1001" /></label>}{operation === 'PUT' && <label className="demo-field">Value<textarea value={value} onChange={e => setValue(e.target.value)} rows={3} /></label>}<button className="demo-button" onClick={execute}>Execute {operation}</button><pre className="demo-response" aria-live="polite">{result}</pre></div><div><h3>Stored in this browser</h3><div className="demo-key-list">{Object.entries(entries).map(([k, v]) => <button key={k} onClick={() => { setKey(k); setValue(v); setOperation('GET') }}><code>{k}</code><span>{v}</span></button>)}{!Object.keys(entries).length && <p>No keys yet. Use PUT to create one.</p>}</div><button className="demo-link" onClick={() => { persist({ ...initialEntries }); setResult('Sample keys restored.'); setEvents(['Sample keys restored · browser demo']) }}><RotateCcw size={13} /> Reset sample data</button></div></div>{route && <div className="demo-route"><span>Key routing preview</span><code>{key}</code><span>CRC32: {route.hashPosition >>> 0}</span><strong>{route.vnode.nodeId}</strong><span>Replicas: {route.replicas.join(', ')}</span></div>}<p className="demo-insight">A key always hashes to a predictable place on the ring. Connect your backend to follow actual replication across your servers.</p></section>}
    {['overview', 'cluster'].includes(section) && cluster}
    {['observability', 'metrics'].includes(section) && <section className="demo-card"><div className="demo-card-heading"><h2>Engine snapshot</h2><Activity size={18} /></div><div className="demo-nodes">{[['Storage', '12 SSTables', 'Compaction combines sorted files to keep reads efficient.'], ['Replication', '0 pending hints', 'Hints let a node catch up after it becomes available again.'], ['Runtime', '48 goroutines', 'Go runs small concurrent tasks for storage and network work.']].map(([name, val, note]) => <div className="demo-node" key={name}><h3>{name}</h3><strong>{val}</strong><p className="demo-note">{note}</p></div>)}</div><h3 className="demo-event-title">Activity preview & your demo actions</h3><ul className="demo-events">{events.map((event, i) => <li key={`${i}-${event}`}>{event}</li>)}</ul><p className="demo-note">Connect your backend to see your own activity and telemetry here.</p></section>}
    {section === 'settings' && <section className="demo-card"><h2>Demo configuration</h2><dl className="demo-config">{[['Mode', 'Browser demo · no network database requests'], ['Storage', 'Local browser storage, with in-memory fallback'], ['Topology', '3 preview nodes · 150 virtual nodes each'], ['Replication', '3 copies in the preview configuration'], ['Data privacy', 'Demo keys stay in this browser; avoid entering secrets'], ['Live backend', 'Local development proxy: HTTP port 9001']].map(([k,v]) => <div key={k}><dt>{k}</dt><dd>{v}</dd></div>)}</dl></section>}
    {setup}<footer className="demo-footer"><Terminal size={14} /> Same idea, two interfaces: use the dashboard visually or kvctl in your terminal.</footer>
  </div>
}
