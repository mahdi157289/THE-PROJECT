import { useState } from 'react'

// Trophy component for ETL layers
const LayerTrophy = ({ layer, size = "w-10 h-10" }) => {
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

export default function ETLPipeline() {
  const [pipelines] = useState([
    {
      id: 1,
      name: 'Bronze Layer',
      description: 'Raw data ingestion and storage',
      status: 'running',
      progress: 85,
      recordsProcessed: 1250000,
      totalRecords: 1500000,
      duration: '2h 15m',
      errors: 0,
      warnings: 2,
      lastRun: '2 hours ago',
      nextRun: 'In 6 hours',
      config: {
        source: 'BVMT Scraper',
        destination: 'PostgreSQL',
        schedule: 'Every 6 hours',
        retention: '30 days'
      }
    },
    {
      id: 2,
      name: 'Silver Layer',
      description: 'Data cleaning and validation',
      status: 'running',
      progress: 67,
      recordsProcessed: 980000,
      totalRecords: 1250000,
      duration: '1h 45m',
      errors: 0,
      warnings: 1,
      lastRun: '1 hour ago',
      nextRun: 'In 5 hours',
      config: {
        source: 'Bronze Layer',
        destination: 'PostgreSQL',
        schedule: 'Every 6 hours',
        retention: '30 days'
      }
    },
    {
      id: 3,
      name: 'Golden Layer',
      description: 'Business logic and transformations',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0m',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'When Silver completes',
      config: {
        source: 'Silver Layer',
        destination: 'PostgreSQL',
        schedule: 'On-demand',
        retention: '90 days'
      }
    },
    {
      id: 4,
      name: 'Diamond Layer',
      description: 'Advanced analytics and ML features',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0m',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'When Golden completes',
      config: {
        source: 'Golden Layer',
        destination: 'PostgreSQL + ML Models',
        schedule: 'Daily',
        retention: '1 year'
      }
    }
  ])

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'text-green-600 bg-green-100'
      case 'completed': return 'text-blue-600 bg-blue-100'
      case 'idle': return 'text-gray-600 bg-gray-100'
      case 'paused': return 'text-yellow-600 bg-yellow-100'
      case 'error': return 'text-red-600 bg-red-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'running': return <span className="w-5 h-5 bg-green-500 rounded-full"></span>
      case 'completed': return <span className="w-5 h-5 bg-blue-500 rounded-full"></span>
      case 'idle': return <span className="w-5 h-5 bg-gray-500 rounded-full"></span>
      case 'paused': return <span className="w-5 h-5 bg-yellow-500 rounded-full"></span>
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

  const handlePipelineAction = (pipelineId, action) => {
    console.log(`Pipeline ${pipelineId}: ${action}`)
    // TODO: Implement actual pipeline control logic
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">ETL Pipeline Management</h1>
          <p className="mt-2 text-gray-600">
            Monitor and control your data pipeline layers
          </p>
        </div>
        <div className="flex gap-3">
          <button className="btn-primary">
            Start All Pipelines
          </button>
          <button className="btn-secondary">
            Stop All Pipelines
          </button>
          <button className="btn-secondary">
            <span className="w-4 h-4 bg-gray-500 rounded mr-2"></span>
            Refresh Status
          </button>
        </div>
      </div>

      {/* Pipeline Cards */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {pipelines.map((pipeline, index) => (
          <div
            key={pipeline.id}
            className="bg-white rounded-lg shadow-md border border-gray-200 p-6 hover:shadow-lg transition-shadow duration-200"
          >
            {/* Header */}
            <div className="flex justify-between items-start mb-4">
              <div className="flex items-center">
                <LayerTrophy layer={getLayerName(pipeline.name)} />
                <div className="ml-3">
                  <h3 className="text-lg font-semibold text-gray-900">{pipeline.name}</h3>
                  <p className="text-sm text-gray-600">{pipeline.description}</p>
                </div>
              </div>
              <span className={`px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(pipeline.status)}`}>
                {pipeline.status}
              </span>
            </div>

            {/* Progress Bar */}
            {pipeline.status === 'running' && (
              <div className="mb-4">
                <div className="flex justify-between text-sm text-gray-600 mb-2">
                  <span>Progress</span>
                  <span>{pipeline.progress}%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-3">
                  <div 
                    className="bg-blue-600 h-3 rounded-full transition-all duration-300"
                    style={{ width: `${pipeline.progress}%` }}
                  ></div>
                </div>
                <div className="flex justify-between text-xs text-gray-500 mt-1">
                  <span>{pipeline.recordsProcessed.toLocaleString()} / {pipeline.totalRecords.toLocaleString()} records</span>
                  <span>{pipeline.duration}</span>
                </div>
              </div>
            )}

            {/* Stats Grid */}
            <div className="grid grid-cols-2 gap-4 mb-4">
              <div className="text-center p-3 bg-gray-50 rounded-lg">
                <p className="text-2xl font-bold text-gray-900">{pipeline.recordsProcessed.toLocaleString()}</p>
                <p className="text-xs text-gray-600">Records Processed</p>
              </div>
              <div className="text-center p-3 bg-gray-50 rounded-lg">
                <p className="text-2xl font-bold text-gray-900">{pipeline.errors}</p>
                <p className="text-xs text-gray-600">Errors</p>
              </div>
            </div>

            {/* Configuration */}
            <div className="mb-4 p-3 bg-blue-50 rounded-lg">
              <h4 className="text-sm font-medium text-blue-900 mb-2">Configuration</h4>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="text-blue-700">Source:</span>
                  <span className="ml-1 text-gray-700">{pipeline.config.source}</span>
                </div>
                <div>
                  <span className="text-blue-700">Schedule:</span>
                  <span className="ml-1 text-gray-700">{pipeline.config.schedule}</span>
                </div>
              </div>
            </div>

            {/* Status Info */}
            <div className="grid grid-cols-2 gap-4 mb-4 text-sm">
              <div>
                <p className="text-gray-600">Last Run</p>
                <p className="font-medium">{pipeline.lastRun}</p>
              </div>
              <div>
                <p className="text-gray-600">Next Run</p>
                <p className="font-medium">{pipeline.nextRun}</p>
              </div>
            </div>

            {/* Action Buttons */}
            <div className="flex gap-2">
              {pipeline.status === 'idle' && (
                <button 
                  onClick={() => handlePipelineAction(pipeline.id, 'start')}
                  className="btn-primary text-sm py-2 px-3"
                >
                  <span className="w-4 h-4 bg-white rounded mr-1"></span>
                  Start
                </button>
              )}
              {pipeline.status === 'running' && (
                <>
                  <button 
                    onClick={() => handlePipelineAction(pipeline.id, 'pause')}
                    className="btn-secondary text-sm py-2 px-3"
                  >
                    <span className="w-4 h-4 bg-gray-500 rounded mr-1"></span>
                    Pause
                  </button>
                  <button 
                    onClick={() => handlePipelineAction(pipeline.id, 'stop')}
                    className="btn-secondary text-sm py-2 px-3"
                  >
                    <span className="w-4 h-4 bg-gray-500 rounded mr-1"></span>
                    Stop
                  </button>
                </>
              )}
              {pipeline.status === 'paused' && (
                <button 
                  onClick={() => handlePipelineAction(pipeline.id, 'resume')}
                  className="btn-primary text-sm py-2 px-3"
                >
                  <span className="w-4 h-4 bg-white rounded mr-1"></span>
                  Resume
                </button>
              )}
              <button 
                onClick={() => handlePipelineAction(pipeline.id, 'restart')}
                className="btn-secondary text-sm py-2 px-3"
              >
                <span className="w-4 h-4 bg-gray-500 rounded mr-1"></span>
                Restart
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Global Pipeline Stats */}
      <div className="bg-white rounded-lg shadow-md border border-gray-200 p-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Pipeline Overview</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
          <div className="text-center">
            <div className="text-3xl font-bold text-green-600">
              {pipelines.filter(p => p.status === 'running').length}
            </div>
            <div className="text-sm text-gray-600">Active Pipelines</div>
          </div>
          <div className="text-center">
            <div className="text-3xl font-bold text-blue-600">
              {pipelines.filter(p => p.status === 'completed').length}
            </div>
            <div className="text-sm text-gray-600">Completed</div>
          </div>
          <div className="text-center">
            <div className="text-3xl font-bold text-yellow-600">
              {pipelines.filter(p => p.status === 'paused').length}
            </div>
            <div className="text-sm text-gray-600">Paused</div>
          </div>
          <div className="text-center">
            <div className="text-3xl font-bold text-red-600">
              {pipelines.filter(p => p.status === 'error').length}
            </div>
            <div className="text-sm text-gray-600">Errors</div>
          </div>
        </div>
      </div>
    </div>
  )
}
