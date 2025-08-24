import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import ETLPipeline from './pages/ETLPipeline'

function App() {
  return (
    <Router>
      <div className="min-h-screen bg-gray-50">
        <Layout>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/etl-pipeline" element={<ETLPipeline />} />
            <Route path="/scraping-monitor" element={
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-4">Scraping Monitor</h2>
                <p className="text-gray-600">Scraping Monitor content coming soon...</p>
              </div>
            } />
            <Route path="/powerbi-dashboards" element={
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-4">Power BI Dashboards</h2>
                <p className="text-gray-600">Power BI content coming soon...</p>
              </div>
            } />
            <Route path="/analytics" element={
              <div className="bg-white rounded-lg shadow-md p-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-4">Analytics</h2>
                <p className="text-gray-600">Analytics content coming soon...</p>
              </div>
            } />
          </Routes>
        </Layout>
      </div>
    </Router>
  )
}

export default App
