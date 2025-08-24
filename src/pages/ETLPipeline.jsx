import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  PlayIcon, 
  StopIcon, 
  ArrowPathIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  PauseIcon
} from '@heroicons/react/24/outline'

// Mock data - will be replaced with real API calls
const mockPipelineData = {
  bronze: {
    status: 'completed',
    progress: 100,
    startTime: '2024-01-15T10:30:00Z',
    endTime: '2024-01-15T10:35:00Z',
    recordsProcessed: 249543,
    errors: 0,
    warnings: 2
  },
  silver: {
    status: 'completed',
    progress: 100,
    startTime: '2024-01-15T10:35:00Z',
    endTime: '2024-01-15T10:40:00Z',
    recordsProcessed: 249543,
    errors: 0,
    warnings: 1
  },
  golden: {
    status: 'completed',
    progress: 100,
    startTime: '2024-01-15T10:40:00Z',
    endTime: '2024-01-15T10:45:00Z',
    recordsProcessed: 249543,
    errors: 0,
    warnings: 0
  },
  diamond: {
    status: 'running',
    progress: 65,
    startTime: '2024-01-15T10:45:00Z',
    endTime: null,
    recordsProcessed: 162200,
    errors: 0,
    warnings: 0
  }
}

const PipelineLayerCard = ({ layer, data, onStart, onStop, onRestart }) => {
  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'text-green-600 bg-green-100'
      case 'running': return 'text-blue-600 bg-blue-100'
      case 'failed': return 'text-red-600 bg-red-100'
      case 'paused': return 'text-yellow-600 bg-yellow-100'
      default: return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed': return <CheckCircleIcon className="h-5 w-5" />
      case 'running': return <ClockIcon className="h-5 w-5" />
      case 'failed': return <ExclamationTriangleIcon className="h-5 w-5" />
      case 'paused': return <PauseIcon className="h-5 w-5" />
      default: return <ClockIcon className="h-5 w-5" />
    }
  }

  const formatDuration = (startTime, endTime) => {
    if (!endTime) return 'Running...'
    const start = new Date(startTime)
    const end = new Date(endTime)
    const diff = end - start
    const minutes = Math.floor(diff / 60000)
    const seconds = Math.floor((diff % 60000) / 1000)
    return `${minutes}m ${seconds}s`
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

      {/* Progress Bar */}
      <div className="mb-4">
        <div className="flex justify-between text-sm text-gray-600 mb-2">
          <span>Progress</span>
          <span>{data.progress}%</span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div 
            className={`h-2 rounded-full transition-all duration-300 ${
              data.status === 'completed' ? 'bg-green-500' : 
              data.status === 'running' ? 'bg-blue-500' : 
              data.status === 'failed' ? 'bg-red-500' : 'bg-gray-500'
            }`}
            style={{ width: `${data.progress}%` }}
          />
        </div>
      </div>

      {/* Pipeline Details */}
      <div className="space-y-3 mb-4">
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Records Processed:</span>
          <span className="text-sm font-medium text-gray-900">{data.recordsProcessed.toLocaleString()}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Duration:</span>
          <span className="text-sm text-gray-900">{formatDuration(data.startTime, data.endTime)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Errors:</span>
          <span className="text-sm font-medium text-gray-900">{data.errors}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Warnings:</span>
          <span className="text-sm font-medium text-gray-900">{data.warnings}</span>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex space-x-2">
        {data.status === 'running' ? (
          <button
            onClick={() => onStop(layer)}
            className="flex items-center px-3 py-2 text-sm font-medium text-red-700 bg-red-100 hover:bg-red-200 rounded-lg transition-colors duration-200"
          >
            <StopIcon className="h-4 w-4 mr-1" />
            Stop
          </button>
        ) : (
          <button
            onClick={() => onStart(layer)}
            className="flex items-center px-3 py-2 text-sm font-medium text-green-700 bg-green-100 hover:bg-green-200 rounded-lg transition-colors duration-200"
          >
            <PlayIcon className="h-4 w-4 mr-1" />
            Start
          </button>
        )}
        
        <button
          onClick={() => onRestart(layer)}
          className="flex items-center px-3 py-2 text-sm font-medium text-blue-700 bg-blue-100 hover:bg-blue-200 rounded-lg transition-colors duration-200"
        >
          <ArrowPathIcon className="h-4 w-4 mr-1" />
          Restart
        </button>
      </div>
    </motion.div>
  )
}

export default function ETLPipeline() {
  const [pipelineData, setPipelineData] = useState(mockPipelineData)
  const [isLoading, setIsLoading] = useState(false)

  // TODO: Replace with real API calls
  const handleStartPipeline = async (layer) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000))
      
      setPipelineData(prev => ({
        ...prev,
        [layer]: {
          ...prev[layer],
          status: 'running',
          progress: 0,
          startTime: new Date().toISOString(),
          endTime: null
        }
      }))
      
      // Simulate progress updates
      const progressInterval = setInterval(() => {
        setPipelineData(prev => {
          const current = prev[layer]
          if (current.progress >= 100) {
            clearInterval(progressInterval)
            return {
              ...prev,
              [layer]: {
                ...current,
                status: 'completed',
                progress: 100,
                endTime: new Date().toISOString()
              }
            }
          }
          return {
            ...prev,
            [layer]: {
              ...current,
              progress: Math.min(current.progress + Math.random() * 20, 100)
            }
          }
        })
      }, 2000)
      
    } catch (error) {
      console.error('Failed to start pipeline:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleStopPipeline = async (layer) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 500))
      
      setPipelineData(prev => ({
        ...prev,
        [layer]: {
          ...prev[layer],
          status: 'paused',
          progress: prev[layer].progress
        }
      }))
    } catch (error) {
      console.error('Failed to stop pipeline:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleRestartPipeline = async (layer) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 500))
      
      setPipelineData(prev => ({
        ...prev,
        [layer]: {
          ...prev[layer],
          status: 'running',
          progress: 0,
          startTime: new Date().toISOString(),
          endTime: null
        }
      }))
    } catch (error) {
      console.error('Failed to restart pipeline:', error)
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">ETL Pipeline Management</h1>
        <p className="mt-2 text-gray-600">
          Monitor and control your Medallion Architecture ETL pipeline layers
        </p>
      </div>

      {/* Pipeline Overview */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {Object.entries(pipelineData).map(([layer, data]) => (
          <PipelineLayerCard
            key={layer}
            layer={layer}
            data={data}
            onStart={handleStartPipeline}
            onStop={handleStopPipeline}
            onRestart={handleRestartPipeline}
          />
        ))}
      </div>

      {/* Global Actions */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Global Pipeline Actions</h2>
        <div className="flex space-x-4">
          <button 
            className="btn-primary"
            disabled={isLoading}
          >
            Start All Pipelines
          </button>
          <button 
            className="btn-secondary"
            disabled={isLoading}
          >
            Stop All Pipelines
          </button>
          <button 
            className="btn-secondary"
            disabled={isLoading}
          >
            Restart All Pipelines
          </button>
        </div>
      </div>

      {/* Pipeline Logs */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Recent Pipeline Logs</h2>
        <div className="bg-gray-50 rounded-lg p-4">
          <p className="text-sm text-gray-600">
            Pipeline logs will be displayed here. This will connect to your existing logging system.
          </p>
        </div>
      </div>
    </div>
  )
}
