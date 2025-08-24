import { useState } from 'react'
import { motion } from 'framer-motion'
import { 
  ChartBarIcon, 
  EyeIcon,
  ArrowTopRightOnSquareIcon
} from '@heroicons/react/24/outline'

// Mock data - will be replaced with real Power BI API calls
const mockDashboards = [
  {
    id: 'market-overview',
    name: 'Market Overview Dashboard',
    description: 'Comprehensive view of market performance, trends, and key indicators',
    lastUpdated: '2024-01-15T10:00:00Z',
    status: 'active',
    thumbnail: '/api/powerbi/thumbnails/market-overview'
  },
  {
    id: 'volatility-analysis',
    name: 'Volatility Analysis Dashboard',
    description: 'Detailed analysis of market volatility patterns and risk metrics',
    lastUpdated: '2024-01-15T09:30:00Z',
    status: 'active',
    thumbnail: '/api/powerbi/thumbnails/volatility-analysis'
  },
  {
    id: 'ml-performance',
    name: 'ML Model Performance Dashboard',
    description: 'Performance metrics and predictions from your machine learning models',
    lastUpdated: '2024-01-15T08:45:00Z',
    status: 'active',
    thumbnail: '/api/powerbi/thumbnails/ml-performance'
  }
]

const DashboardCard = ({ dashboard, onView, onEmbed }) => {
  return (
    <motion.div
      whileHover={{ scale: 1.02 }}
      className="card"
    >
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center">
          <div className="p-2 bg-blue-100 rounded-lg mr-3">
            <ChartBarIcon className="h-6 w-6 text-blue-600" />
          </div>
          <div>
            <h3 className="text-lg font-semibold text-gray-900">{dashboard.name}</h3>
            <p className="text-sm text-gray-600">{dashboard.description}</p>
          </div>
        </div>
        <span className="px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-600">
          {dashboard.status}
        </span>
      </div>

      <div className="mb-4">
        <p className="text-sm text-gray-600">
          Last Updated: {new Date(dashboard.lastUpdated).toLocaleString()}
        </p>
      </div>

      <div className="flex space-x-2">
        <button
          onClick={() => onView(dashboard)}
          className="flex items-center px-3 py-2 text-sm font-medium text-blue-700 bg-blue-100 hover:bg-blue-200 rounded-lg transition-colors duration-200"
        >
          <EyeIcon className="h-4 w-4 mr-1" />
          View
        </button>
        <button
          onClick={() => onEmbed(dashboard)}
          className="flex items-center px-3 py-2 text-sm font-medium text-green-700 bg-green-100 hover:bg-green-200 rounded-lg transition-colors duration-200"
        >
          <ArrowTopRightOnSquareIcon className="h-4 w-4 mr-1" />
          Embed
        </button>
      </div>
    </motion.div>
  )
}

export default function PowerBIDashboards() {
  const [dashboards, setDashboards] = useState(mockDashboards)
  const [selectedDashboard, setSelectedDashboard] = useState(null)
  const [isEmbedded, setIsEmbedded] = useState(false)

  const handleViewDashboard = (dashboard) => {
    // TODO: Open Power BI dashboard in new tab or modal
    console.log('Viewing dashboard:', dashboard.name)
    setSelectedDashboard(dashboard)
  }

  const handleEmbedDashboard = (dashboard) => {
    // TODO: Embed Power BI dashboard in the page
    console.log('Embedding dashboard:', dashboard.name)
    setIsEmbedded(true)
    setSelectedDashboard(dashboard)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Power BI Dashboards</h1>
        <p className="mt-2 text-gray-600">
          Access and embed your existing Power BI dashboards for financial analysis
        </p>
      </div>

      {/* Dashboard Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {dashboards.map((dashboard) => (
          <DashboardCard
            key={dashboard.id}
            dashboard={dashboard}
            onView={handleViewDashboard}
            onEmbed={handleEmbedDashboard}
          />
        ))}
      </div>

      {/* Embedded Dashboard View */}
      {isEmbedded && selectedDashboard && (
        <div className="card">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xl font-semibold text-gray-900">
              {selectedDashboard.name}
            </h2>
            <button
              onClick={() => setIsEmbedded(false)}
              className="text-gray-400 hover:text-gray-600"
            >
              ×
            </button>
          </div>
          
          <div className="bg-gray-50 rounded-lg p-8 text-center">
            <ChartBarIcon className="h-16 w-16 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-600 mb-4">
              Power BI dashboard will be embedded here
            </p>
            <p className="text-sm text-gray-500">
              This will connect to your Power BI Service API to display the actual dashboard
            </p>
          </div>
        </div>
      )}

      {/* Power BI Integration Info */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Power BI Integration</h2>
        <div className="space-y-4">
          <div className="bg-blue-50 rounded-lg p-4">
            <h3 className="font-medium text-blue-900 mb-2">Integration Status</h3>
            <p className="text-sm text-blue-700">
              Ready to connect to Power BI Service. You'll need to configure authentication and API access.
            </p>
          </div>
          
          <div className="bg-gray-50 rounded-lg p-4">
            <h3 className="font-medium text-gray-900 mb-2">Next Steps</h3>
            <ul className="text-sm text-gray-600 space-y-1">
              <li>• Configure Power BI REST API authentication</li>
              <li>• Set up dashboard embedding permissions</li>
              <li>• Connect to your existing Power BI workspace</li>
              <li>• Test dashboard embedding functionality</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  )
}
