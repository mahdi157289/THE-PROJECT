import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  PlayIcon, 
  StopIcon, 
  ArrowPathIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  DocumentTextIcon
} from '@heroicons/react/24/outline'

// Mock data - will be replaced with real API calls
const mockScrapingData = {
  cotations: {
    status: 'completed',
    lastRun: '2024-01-15T09:00:00Z',
    recordsScraped: 15680,
    duration: '15m 30s',
    errors: 0,
    warnings: 2,
    filesGenerated: ['2008_histo_cotations.csv', '2009_histo_cotations.csv', '2024_histo_cotations.csv']
  },
  indices: {
    status: 'completed',
    lastRun: '2024-01-15T09:15:00Z',
    recordsScraped: 15680,
    duration: '12m 45s',
    errors: 0,
    warnings: 1,
    filesGenerated: ['2008_histo_indices.csv', '2009_histo_indices.csv', '2024_histo_indices.csv']
  }
}

const ScrapingJobCard = ({ type, data, onStart, onStop, onRestart }) => {
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
        <h3 className="text-lg font-semibold text-gray-900 capitalize">{type} Scraping</h3>
        <span className={`px-2 py-1 rounded-full text-xs font-medium ${getStatusColor(data.status)}`}>
          {data.status}
        </span>
      </div>

      {/* Job Details */}
      <div className="space-y-3 mb-4">
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Records Scraped:</span>
          <span className="text-sm font-medium text-gray-900">{data.recordsScraped.toLocaleString()}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Last Run:</span>
          <span className="text-sm text-gray-900">
            {new Date(data.lastRun).toLocaleString()}
          </span>
        </div>
        <div className="flex justify-between">
          <span className="text-sm text-gray-600">Duration:</span>
          <span className="text-sm text-gray-900">{data.duration}</span>
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

      {/* Files Generated */}
      <div className="mb-4">
        <h4 className="text-sm font-medium text-gray-700 mb-2">Files Generated:</h4>
        <div className="space-y-1">
          {data.filesGenerated.slice(0, 3).map((file, index) => (
            <div key={index} className="flex items-center text-sm text-gray-600">
              <DocumentTextIcon className="h-4 w-4 mr-2" />
              {file}
            </div>
          ))}
          {data.filesGenerated.length > 3 && (
            <p className="text-xs text-gray-500">+{data.filesGenerated.length - 3} more files</p>
          )}
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex space-x-2">
        {data.status === 'running' ? (
          <button
            onClick={() => onStop(type)}
            className="flex items-center px-3 py-2 text-sm font-medium text-red-700 bg-red-100 hover:bg-red-200 rounded-lg transition-colors duration-200"
          >
            <StopIcon className="h-4 w-4 mr-1" />
            Stop
          </button>
        ) : (
          <button
            onClick={() => onStart(type)}
            className="flex items-center px-3 py-2 text-sm font-medium text-green-700 bg-green-100 hover:bg-green-200 rounded-lg transition-colors duration-200"
          >
            <PlayIcon className="h-4 w-4 mr-1" />
            Start
          </button>
        )}
        
        <button
          onClick={() => onRestart(type)}
          className="flex items-center px-3 py-2 text-sm font-medium text-blue-700 bg-blue-100 hover:bg-blue-200 rounded-lg transition-colors duration-200"
        >
          <ArrowPathIcon className="h-4 w-4 mr-1" />
          Restart
        </button>
      </div>
    </motion.div>
  )
}

export default function ScrapingMonitor() {
  const [scrapingData, setScrapingData] = useState(mockScrapingData)
  const [isLoading, setIsLoading] = useState(false)

  // TODO: Replace with real API calls
  const handleStartScraping = async (type) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 1000))
      
      setScrapingData(prev => ({
        ...prev,
        [type]: {
          ...prev[type],
          status: 'running',
          lastRun: new Date().toISOString()
        }
      }))
      
      // Simulate completion after some time
      setTimeout(() => {
        setScrapingData(prev => ({
          ...prev,
          [type]: {
            ...prev[type],
            status: 'completed',
            recordsScraped: Math.floor(Math.random() * 5000) + 15000,
            duration: `${Math.floor(Math.random() * 10) + 10}m ${Math.floor(Math.random() * 30)}s`
          }
        }))
      }, 5000)
      
    } catch (error) {
      console.error('Failed to start scraping:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleStopScraping = async (type) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 500))
      
      setScrapingData(prev => ({
        ...prev,
        [type]: {
          ...prev[type],
          status: 'stopped'
        }
      }))
    } catch (error) {
      console.error('Failed to stop scraping:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const handleRestartScraping = async (type) => {
    setIsLoading(true)
    try {
      // Simulate API call
      await new Promise(resolve => setTimeout(resolve, 500))
      
      setScrapingData(prev => ({
        ...prev,
        [type]: {
          ...prev[type],
          status: 'running',
          lastRun: new Date().toISOString()
        }
      }))
    } catch (error) {
      console.error('Failed to restart scraping:', error)
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">BVMT Scraping Monitor</h1>
        <p className="mt-2 text-gray-600">
          Monitor and control your BVMT data scraping operations for cotations and indices
        </p>
      </div>

      {/* Scraping Jobs */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {Object.entries(scrapingData).map(([type, data]) => (
          <ScrapingJobCard
            key={type}
            type={type}
            data={data}
            onStart={handleStartScraping}
            onStop={handleStopScraping}
            onRestart={handleRestartScraping}
          />
        ))}
      </div>

      {/* Global Actions */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Global Scraping Actions</h2>
        <div className="flex space-x-4">
          <button 
            className="btn-primary"
            disabled={isLoading}
          >
            Start All Scraping Jobs
          </button>
          <button 
            className="btn-secondary"
            disabled={isLoading}
          >
            Stop All Scraping Jobs
          </button>
          <button 
            className="btn-secondary"
            disabled={isLoading}
          >
            View Scraped Files
          </button>
        </div>
      </div>

      {/* Scraping History */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Scraping History</h2>
        <div className="bg-gray-50 rounded-lg p-4">
          <p className="text-sm text-gray-600">
            Scraping history and logs will be displayed here. This will connect to your existing BVMT scraper.
          </p>
        </div>
      </div>
    </div>
  )
}
