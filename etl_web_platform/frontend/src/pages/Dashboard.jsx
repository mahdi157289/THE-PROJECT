import { useState } from 'react'

// Trophy component for ETL layers
const LayerTrophy = ({ layer, size = "w-8 h-8" }) => {
  const getTrophyStyle = (layer) => {
    switch (layer.toLowerCase()) {
      case 'bronze':
        return `text-amber-600 ${size}`
      case 'silver':
        return `text-gray-500 ${size}`
      case 'golden':
        return `text-yellow-500 ${size}`
      case 'diamond':
        return `text-blue-600 ${size}`
      default:
        return `text-gray-500 ${size}`
    }
  }

  return (
    <div className={getTrophyStyle(layer)}>
      <svg fill="currentColor" viewBox="0 0 20 20">
        <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
      </svg>
    </div>
  )
}

export default function Dashboard() {
  // Mock data - will be replaced with real API calls
  const [stats] = useState({
    totalRecords: 1250000,
    activePipelines: 4,
    cpuUsage: 45,
    memoryUsage: 62
  })

  const [pipelineStatus] = useState([
    { name: 'Bronze Layer', status: 'running', progress: 85, records: 1250000, duration: '2h 15m', errors: 0, warnings: 2 },
    { name: 'Silver Layer', status: 'running', progress: 67, records: 980000, duration: '1h 45m', errors: 0, warnings: 1 },
    { name: 'Golden Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
    { name: 'Diamond Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 }
  ])

  const [scrapingStatus] = useState([
    { name: 'BVMT Cotations', status: 'completed', lastRun: '2 hours ago', records: 15000, duration: '45m', errors: 0, warnings: 1 },
    { name: 'BVMT Indices', status: 'completed', lastRun: '2 hours ago', records: 5000, duration: '20m', errors: 0, warnings: 0 }
  ])

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'text-green-600 bg-green-100'
      case 'completed': return 'text-blue-600 bg-blue-100'
      case 'idle': return 'text-gray-600 bg-gray-100'
      case 'error': return 'text-red-600 bg-red-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'running': return <span className="w-5 h-5 bg-green-500 rounded-full"></span>
      case 'completed': return <span className="w-5 h-5 bg-blue-500 rounded-full"></span>
      case 'idle': return <span className="w-5 h-5 bg-gray-500 rounded-full"></span>
      case 'error': return <span className="w-5 h-5 bg-red-500 rounded-full"></span>
      default: return <span className="w-5 h-5 bg-gray-500 rounded-full"></span>
    }
  }

  // Extract layer name for trophy
  const getLayerName = (fullName) => {
    if (fullName.includes('Bronze')) return 'Bronze'
    if (fullName.includes('Silver')) return 'Silver'
    if (fullName.includes('Golden')) return 'Golden'
    if (fullName.includes('Diamond')) return 'Diamond'
    return 'Unknown'
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">ETL Platform Dashboard</h1>
        <p className="mt-2 text-gray-600">
          Monitor your financial data pipeline, scraping operations, and system health
        </p>
        
        {/* ETL Layer Legend */}
        <div className="mt-4 p-4 bg-gradient-to-r from-gray-50 to-blue-50 rounded-lg border border-gray-200">
          <h3 className="text-sm font-semibold text-gray-700 mb-3">ETL Layer Legend</h3>
          <div className="flex flex-wrap gap-4">
            <div className="flex items-center">
              <LayerTrophy layer="Bronze" size="w-6 h-6" />
              <span className="ml-2 text-sm text-gray-600">Bronze - Raw Data Ingestion</span>
            </div>
            <div className="flex items-center">
              <LayerTrophy layer="Silver" size="w-6 h-6" />
              <span className="ml-2 text-sm text-gray-600">Silver - Data Cleaning</span>
            </div>
            <div className="flex items-center">
              <LayerTrophy layer="Golden" size="w-6 h-6" />
              <span className="ml-2 text-sm text-gray-600">Golden - Business Logic</span>
            </div>
            <div className="flex items-center">
              <LayerTrophy layer="Diamond" size="w-6 h-6" />
              <span className="ml-2 text-sm text-gray-600">Diamond - ML & Analytics</span>
            </div>
          </div>
        </div>
      </div>

      {/* Overview Stats */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6 hover:shadow-lg transition-shadow duration-200">
          <div className="flex items-center">
            <span className="w-8 h-8 bg-blue-500 rounded"></span>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Records</p>
              <p className="text-2xl font-bold text-gray-900">{stats.totalRecords.toLocaleString()}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6 hover:shadow-lg transition-shadow duration-200">
          <div className="flex items-center">
            <span className="w-8 h-8 bg-green-500 rounded"></span>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Active Pipelines</p>
              <p className="text-2xl font-bold text-gray-900">{stats.activePipelines}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6 hover:shadow-lg transition-shadow duration-200">
          <div className="flex items-center">
            <span className="w-8 h-8 bg-yellow-500 rounded"></span>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">CPU Usage</p>
              <p className="text-2xl font-bold text-gray-900">{stats.cpuUsage}%</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6 hover:shadow-lg transition-shadow duration-200">
          <div className="flex items-center">
            <span className="w-8 h-8 bg-purple-500 rounded"></span>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Memory Usage</p>
              <p className="text-2xl font-bold text-gray-900">{stats.memoryUsage}%</p>
            </div>
          </div>
        </div>
      </div>

      {/* Pipeline Status */}
      <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">ETL Pipeline Status</h2>
        <div className="space-y-4">
          {pipelineStatus.map((pipeline, index) => (
            <div
              key={pipeline.name}
              className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow duration-200"
            >
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center">
                  <LayerTrophy layer={getLayerName(pipeline.name)} />
                  <div className="ml-3 flex items-center">
                    {getStatusIcon(pipeline.status)}
                    <span className="ml-2 font-medium text-gray-900">{pipeline.name}</span>
                  </div>
                </div>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(pipeline.status)}`}>
                  {pipeline.status}
                </span>
              </div>
              
              {pipeline.status === 'running' && (
                <div className="mb-2">
                  <div className="flex justify-between text-sm text-gray-600 mb-1">
                    <span>Progress</span>
                    <span>{pipeline.progress}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2">
                    <div 
                      className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                      style={{ width: `${pipeline.progress}%` }}
                    ></div>
                  </div>
                </div>
              )}
              
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-gray-600">Records</p>
                  <p className="font-medium">{pipeline.records.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-gray-600">Duration</p>
                  <p className="font-medium">{pipeline.duration}</p>
                </div>
                <div>
                  <p className="text-gray-600">Errors</p>
                  <p className="font-medium text-red-600">{pipeline.errors}</p>
                </div>
                <div>
                  <p className="text-gray-600">Warnings</p>
                  <p className="font-medium text-yellow-600">{pipeline.warnings}</p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Scraping Status */}
      <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Scraping Status</h2>
        <div className="space-y-4">
          {scrapingStatus.map((job, index) => (
            <div
              key={job.name}
              className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow duration-200"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="font-medium text-gray-900">{job.name}</span>
                <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(job.status)}`}>
                  {job.status}
                </span>
              </div>
              
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-gray-600">Last Run</p>
                  <p className="font-medium">{job.lastRun}</p>
                </div>
                <div>
                  <p className="text-gray-600">Records</p>
                  <p className="font-medium">{job.records.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-gray-600">Duration</p>
                  <p className="font-medium">{job.duration}</p>
                </div>
                <div>
                  <p className="text-gray-600">Errors/Warnings</p>
                  <p className="font-medium">
                    <span className="text-red-600">{job.errors}</span>
                    <span className="text-gray-400 mx-1">/</span>
                    <span className="text-yellow-600">{job.warnings}</span>
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Quick Actions */}
      <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Quick Actions</h2>
        <div className="flex flex-wrap gap-4">
          <button className="btn-primary">
            Start All Pipelines
          </button>
          <button className="btn-secondary">
            Stop All Pipelines
          </button>
          <button className="btn-secondary">
            Run Scraping Jobs
          </button>
          <button className="btn-secondary">
            View Logs
          </button>
        </div>
      </div>
    </div>
  )
}
