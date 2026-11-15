import { useState, useEffect } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { Database, Network, BarChart3, Settings, Book, Activity, PanelLeftClose, PanelLeftOpen } from 'lucide-react'
import { Link } from 'react-router-dom'
import { KVOperations } from '../components/dashboard/KVOperations'
import { ClusterStatus } from '../components/dashboard/ClusterStatus'
import { MetricsView } from '../components/dashboard/MetricsView'
import { SettingsPanel } from '../components/dashboard/SettingsPanel'
import { ObservabilityPanel } from '../components/dashboard/ObservabilityPanel'

import { DemoDashboard } from '../components/dashboard/DemoDashboard'

const API_BASE = import.meta.env.VITE_API_URL || ''

interface NavItem {
  id: string
  label: string
  icon: typeof Database
}

const NAV_ITEMS: NavItem[] = [
  { id: 'overview', label: 'Overview', icon: Database },
  { id: 'operations', label: 'Key-Value Store', icon: Database },
  { id: 'cluster', label: 'Cluster', icon: Network },
  { id: 'metrics', label: 'Metrics', icon: BarChart3 },
  { id: 'observability', label: 'Observability', icon: Activity },
  { id: 'settings', label: 'Settings', icon: Settings },
]

export function Dashboard() {
  const [activeNav, setActiveNav] = useState('overview')
  const [demoMode, setDemoMode] = useState(true)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [backendState, setBackendState] = useState<'checking' | 'ready' | 'setup'>('checking')
  const [retry, setRetry] = useState(0)
  useEffect(() => {
    if (demoMode) return
    let cancelled = false
    const controller = new AbortController()
    const timer = window.setTimeout(() => controller.abort(), 5000)
    setBackendState('checking')
    async function check() {
      try {
        const response = await fetch(`${API_BASE.startsWith('http') ? API_BASE : ''}/health`, { signal: controller.signal })
        const body = await response.json()
        if (!cancelled) setBackendState(response.ok && body.success === true && body.data?.status === 'healthy' ? 'ready' : 'setup')
      } catch { if (!cancelled) setBackendState('setup') }
      finally { window.clearTimeout(timer) }
    }
    check()
    return () => { cancelled = true; window.clearTimeout(timer); controller.abort() }
  }, [demoMode, retry, activeNav])


  return (
    <div className="dashboard">
      <aside className={`dashboard-sidebar ${sidebarCollapsed ? 'collapsed' : ''}`}>
        <div className="sidebar-header">
          <Link to="/" className="sidebar-logo" aria-label="TurboDynamo home" title="Back to landing page">
            <span className="sidebar-logo-text">{sidebarCollapsed ? 'TD' : 'turbodynamo'}</span>
          </Link>
          <button
            className="sidebar-collapse-toggle"
            aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
            aria-expanded={!sidebarCollapsed}
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
          >
            {sidebarCollapsed ? <PanelLeftOpen size={18} /> : <PanelLeftClose size={18} />}
          </button>
        </div>

        <nav className="sidebar-nav">
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon
            return (
              <button
                key={item.id}
                className={`sidebar-nav-item ${activeNav === item.id ? 'active' : ''}`}
                onClick={() => setActiveNav(item.id)}
                title={item.label}
              >
                <span className="sidebar-nav-icon">
                  <Icon size={18} strokeWidth={1.8} />
                </span>
                {!sidebarCollapsed && (
                  <span className="sidebar-nav-label">{item.label}</span>
                )}
              </button>
            )
          })}

          <div style={{ marginTop: 'auto', paddingTop: '20px', borderTop: '1px solid var(--border)' }}>
            <Link
              to="/docs"
              className="sidebar-nav-item"
              style={{ color: 'var(--text-muted)' }}
              title="Documentation"
            >
              <span className="sidebar-nav-icon">
                <Book size={18} strokeWidth={1.8} />
              </span>
              {!sidebarCollapsed && (
                <span className="sidebar-nav-label">Docs</span>
              )}
            </Link>
          </div>
        </nav>
      </aside>

      <main className={`dashboard-main ${sidebarCollapsed ? 'collapsed' : ''}`}>
        <div className="dashboard-mode-switch" role="group" aria-label="Dashboard mode">
          <button aria-pressed={demoMode} onClick={() => setDemoMode(true)}>Demo mode</button>
          <button aria-pressed={!demoMode} onClick={() => { setDemoMode(false); if (activeNav === 'overview') setActiveNav('operations') }}>Live backend</button>
          <span>{demoMode ? 'Explore without a server' : 'Connects to your configured API · server required'}</span>
        </div>
        <AnimatePresence mode="wait">
          <motion.div
            key={demoMode ? "demo" : activeNav}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.2 }}
            className="dashboard-content"
          >
            {!demoMode && backendState !== 'ready' && <section className="demo-card demo-backend-setup">
              <span className="demo-eyebrow">YOUR OWN DATA, CONNECTED</span>
              <h1>{backendState === 'checking' ? 'Checking your backend…' : 'Bring your dashboard to life.'}</h1>
              <p>Demo mode lets you explore right away. To see your own keys, cluster, and live metrics, run TurboDynamo locally or connect a hosted server.</p>
              <ol><li>Follow the local installation guide to start the Go server.</li><li>Start the frontend locally so it can reach your server.</li><li>Return to Live backend and check the connection.</li></ol>
              <div className="demo-tabs"><button className="demo-button" onClick={() => setDemoMode(true)}>Explore demo mode</button><button className="demo-button" disabled={backendState === 'checking'} onClick={() => setRetry(n => n + 1)}>Check connection</button></div>
              <Link className="demo-link" to="/docs#installation">Open local setup documentation</Link>
              <p className="demo-note">On a frontend-only deployment, use Demo mode. A local server connects through your locally running frontend.</p>
            </section>}
            {demoMode && <DemoDashboard section={activeNav} onNavigate={setActiveNav} />}
            {!demoMode && backendState === 'ready' && activeNav === 'overview' && <p>Select a section to view your live backend.</p>}
            {!demoMode && backendState === 'ready' && activeNav === 'operations' && <KVOperations apiBase={API_BASE} />}
            {!demoMode && backendState === 'ready' && activeNav === 'cluster' && <ClusterStatus apiBase={API_BASE} />}
            {!demoMode && backendState === 'ready' && activeNav === 'metrics' && <MetricsView apiBase={API_BASE} />}
            {!demoMode && backendState === 'ready' && activeNav === 'observability' && <ObservabilityPanel apiBase={API_BASE} />}
            {!demoMode && backendState === 'ready' && activeNav === 'settings' && <SettingsPanel apiBase={API_BASE} />}
          </motion.div>
        </AnimatePresence>
      </main>
    </div>
  )
}
