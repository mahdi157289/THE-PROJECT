import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  DatabaseIcon, 
  ChartBarIcon, 
  CogIcon, 
  CheckCircleIcon,
  ExclamationTriangleIcon,
  ClockIcon
} from '@heroicons/react/24/outline'

// Mock data - will be replaced with real API calls
const mockData = {
  pipelineStatus: {
    bronze: { status: 'completed', records: 249543, lastRun: '2024-01-15T10:30:00Z' },
    silver: { status: 'completed', records: 249543, lastRun: '2024-01-15T10:35:00Z' },
    golden: { status: 'completed', records: 249543, lastRun: '2024-01-15T10:40:00Z' },
    diamond: { status: 'running', records: 0, lastRun: '2024-01-15T10:45:00Z' }
  },
  scrapingStatus: {
    cotations: { status: 'completed', lastRun: '2024-01-15T09:00:00Z', records: 15680 },
    indices: { status: 'completed', lastRun: '2024-01-15T09:15:00Z', records: 15680 }
  },
  systemHealth: {
    cpu: 45,
    memory: 62,
    disk: 78,
    activePipelines: 1
  }
}

const StatCard = ({ title, value, icon: Icon, status, trend }) => (
  <motion.div
    whileHover={{ scale: 1.02 }}
    className="card"
  >
    <div className="flex items-center justify-between">
      <div>
        <p className="text-sm font-medium text-gray-600">{title}</p>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
        {trend && (
          <p className={`text-sm ${trend > 0 ? 'text-green-600' : 'text-red-600'}`}>
            {trend > 0 ? '+' : ''}{trend}% from last week
          </p>
        )}
      </div>
      <div className={`p-3 rounded-full ${status === 'completed' ? 'bg-green-100' : status === 'running' ? 'bg-blue-100' : 'bg-red-100'}`}>
        <Icon className={`h-6 w-6 ${status === 'completed' ? 'text-green-600' : status === 'running' ? 'text-blue-600' : 'text-red-600'}`} />
      </div>
    </div>
  </motion.div>
)

const PipelineStatusCard = ({ layer, data }) => {
  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'text-green-600 bg-green-100'
      case 'running': return 'text-blue-600 bg-blue-100'
      case 'failed': return 'text-red-600 bg-red-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed': return <CheckCircleIcon className="h-5 w-5" />
      case 'running': return <ClockIcon className="h-5 w-5" />
      case 'failed': return <ExclamationTriangleIcon className="h-5 w-5" />
      default: return <ClockIcon className="h-5 w-5" />
    }
  }

  return (
    <motion.div
      whileHover={{ scale: 1.02 }}
      className="card"
    >
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-900 capitalize">{layer} Layer</h3>
        <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(data.status)}`}>
          {data.status}
        </span>
      </div>
      
      <div className="space-y-3">
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Records Processed:</span>
          <span className="text-sm font-medium text-gray-900">{data.records.toLocaleString()}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Last Run:</span>
          <span className="text-sm text-gray-900">
            {new Date(data.lastRun).toLocaleString()}
          </span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-sm text-gray-600">Status:</span>
          <div className="flex items-center space-x-1">
            {getStatusIcon(data.status)}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

export default function Dashboard() {
  const [data, setData] = useState(mockData)

  // TODO: Replace with real API calls
  useEffect(() => {
    // fetchDashboardData()
  }, [])

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">ETL Platform Dashboard</h1>
        <p className="mt-2 text-gray-600">
          Monitor your financial data pipeline, scraping operations, and system health
        </p>
      </div>

      {/* Overview Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Total Records"
          value="249,543"
          icon={DatabaseIcon}
          status="completed"
          trend={12}
        />
        <StatCard
          title="Active Pipelines"
          value={data.systemHealth.activePipelines}
          icon={CogIcon}
          status="running"
        />
        <StatCard
          title="CPU Usage"
          value={`${data.systemHealth.cpu}%`}
          icon={ChartBarIcon}
          status="completed"
        />
        <StatCard
          title="Memory Usage"
          value={`${data.systemHealth.memory}%`}
          icon={ChartBarIcon}
          status="completed"
        />
      </div>

      {/* Pipeline Status */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Pipeline Status</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {Object.entries(data.pipelineStatus).map(([layer, layerData]) => (
            <PipelineStatusCard key={layer} layer={layer} data={layerData} />
          ))}
        </div>
      </div>

      {/* Scraping Status */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Scraping Status</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {Object.entries(data.scrapingStatus).map(([type, typeData]) => (
            <motion.div
              key={type}
              whileHover={{ scale: 1.02 }}
              className="card"
            >
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-gray-900 capitalize">{type}</h3>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(typeData.status)}`}>
                  {typeData.status}
                </span>
              </div>
              
              <div className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">Records Scraped:</span>
                  <span className="text-sm font-medium text-gray-900">{typeData.records.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-sm text-gray-600">Last Run:</span>
                  <span className="text-sm text-gray-900">
                    {new Date(typeData.lastRun).toLocaleString()}
                  </span>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Quick Actions */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Quick Actions</h2>
        <div className="flex space-x-4">
          <button className="btn-primary">
            Start ETL Pipeline
          </button>
          <button className="btn-secondary">
            Start Scraping
          </button>
          <button className="btn-secondary">
            View Logs
          </button>
        </div>
      </div>
    </div>
  )
}

// Helper function for status colors
function getStatusColor(status) {
  switch (status) {
    case 'completed': return 'text-green-600 bg-green-100'
    case 'running': return 'text-blue-600 bg-blue-100'
    case 'failed': return 'text-red-600 bg-red-100'
    default: return 'text-gray-600 bg-gray-100'
  }
}
