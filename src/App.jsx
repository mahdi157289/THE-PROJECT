import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import ETLPipeline from './pages/ETLPipeline'
import ScrapingMonitor from './pages/ScrapingMonitor'
import PowerBIDashboards from './pages/PowerBIDashboards'
import Analytics from './pages/Analytics'

function App() {
  return (
    <Router>
      <div className="min-h-screen bg-gray-50">
        <Layout>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/etl-pipeline" element={<ETLPipeline />} />
            <Route path="/scraping-monitor" element={<ScrapingMonitor />} />
            <Route path="/powerbi-dashboards" element={<PowerBIDashboards />} />
            <Route path="/analytics" element={<Analytics />} />
          </Routes>
        </Layout>
        <Toaster 
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: '#363636',
              color: '#fff',
            },
          }}
        />
      </div>
    </Router>
  )
}

export default App
