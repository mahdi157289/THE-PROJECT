import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { 
  ChartBarIcon, 
  TrendingUpIcon,
  TrendingDownIcon,
  CalculatorIcon,
  CpuChipIcon
} from '@heroicons/react/24/outline'

// Mock data - will be replaced with real API calls
const mockAnalyticsData = {
  marketMetrics: {
    totalVolume: 15420.5,
    averagePrice: 125.75,
    volatilityIndex: 0.18,
    trendDirection: 'up',
    changePercent: 2.3
  },
  mlPredictions: {
    nextDayPrediction: 127.85,
    confidence: 0.87,
    modelAccuracy: 0.92,
    lastTraining: '2024-01-14T15:00:00Z'
  },
  dataQuality: {
    completeness: 98.5,
    accuracy: 97.2,
    consistency: 99.1,
    timeliness: 95.8
  }
}

const MetricCard = ({ title, value, icon: Icon, trend, subtitle }) => (
  <motion.div
    whileHover={{ scale: 1.02 }}
    className="card"
  >
    <div className="flex items-center justify-between">
      <div>
        <p className="text-sm font-medium text-gray-600">{title}</p>
        <p className="text-2xl font-bold text-gray-900">{value}</p>
        {subtitle && (
          <p className="text-sm text-gray-500">{subtitle}</p>
        )}
      </div>
      <div className="p-3 rounded-full bg-blue-100">
        <Icon className="h-6 w-6 text-blue-600" />
      </div>
    </div>
    {trend && (
      <div className="mt-3 flex items-center">
        {trend > 0 ? (
          <TrendingUpIcon className="h-4 w-4 text-green-500 mr-1" />
        ) : (
          <TrendingDownIcon className="h-4 w-4 text-red-500 mr-1" />
        )}
        <span className={`text-sm ${trend > 0 ? 'text-green-600' : 'text-red-600'}`}>
          {trend > 0 ? '+' : ''}{trend}%
        </span>
      </div>
    )}
  </motion.div>
)

const QualityMetricCard = ({ metric, value, color }) => (
  <motion.div
    whileHover={{ scale: 1.02 }}
    className="card"
  >
    <div className="text-center">
      <h3 className="text-lg font-semibold text-gray-900 mb-2">{metric}</h3>
      <div className="relative">
        <div className="w-24 h-24 mx-auto mb-4">
          <svg className="w-full h-full transform -rotate-90" viewBox="0 0 36 36">
            <path
              className="text-gray-200"
              strokeWidth="3"
              fill="none"
              d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
            />
            <path
              className={color}
              strokeWidth="3"
              strokeDasharray={`${value}, 100`}
              fill="none"
              d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
            />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <span className="text-xl font-bold text-gray-900">{value}%</span>
          </div>
        </div>
      </div>
    </div>
  </motion.div>
)

export default function Analytics() {
  const [analyticsData, setAnalyticsData] = useState(mockAnalyticsData)
  const [isLoading, setIsLoading] = useState(false)

  // TODO: Replace with real API calls
  useEffect(() => {
    // fetchAnalyticsData()
  }, [])

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Financial Analytics</h1>
        <p className="mt-2 text-gray-600">
          Real-time financial data analysis, ML predictions, and data quality metrics
        </p>
      </div>

      {/* Market Metrics */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Market Metrics</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <MetricCard
            title="Total Volume"
            value={`$${analyticsData.marketMetrics.totalVolume.toLocaleString()}`}
            icon={ChartBarIcon}
            trend={analyticsData.marketMetrics.changePercent}
            subtitle="Million"
          />
          <MetricCard
            title="Average Price"
            value={`$${analyticsData.marketMetrics.averagePrice}`}
            icon={CalculatorIcon}
            subtitle="Per Share"
          />
          <MetricCard
            title="Volatility Index"
            value={analyticsData.marketMetrics.volatilityIndex}
            icon={TrendingUpIcon}
            subtitle="Risk Measure"
          />
          <MetricCard
            title="Trend Direction"
            value={analyticsData.marketMetrics.trendDirection.toUpperCase()}
            icon={analyticsData.marketMetrics.trendDirection === 'up' ? TrendingUpIcon : TrendingDownIcon}
            subtitle="Market Movement"
          />
        </div>
      </div>

      {/* ML Model Performance */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Machine Learning Model Performance</h2>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <motion.div
            whileHover={{ scale: 1.02 }}
            className="card"
          >
            <div className="flex items-center mb-4">
              <div className="p-2 bg-purple-100 rounded-lg mr-3">
                <CpuChipIcon className="h-6 w-6 text-purple-600" />
              </div>
              <h3 className="text-lg font-semibold text-gray-900">Next Day Prediction</h3>
            </div>
            
            <div className="space-y-4">
              <div className="text-center">
                <p className="text-3xl font-bold text-purple-600">
                  ${analyticsData.mlPredictions.nextDayPrediction}
                </p>
                <p className="text-sm text-gray-600">Predicted Price</p>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-sm text-gray-600">Confidence</p>
                  <p className="text-lg font-semibold text-gray-900">
                    {(analyticsData.mlPredictions.confidence * 100).toFixed(1)}%
                  </p>
                </div>
                <div>
                  <p className="text-sm text-gray-600">Model Accuracy</p>
                  <p className="text-lg font-semibold text-gray-900">
                    {(analyticsData.mlPredictions.modelAccuracy * 100).toFixed(1)}%
                  </p>
                </div>
              </div>
              
              <div className="text-sm text-gray-600">
                Last Training: {new Date(analyticsData.mlPredictions.lastTraining).toLocaleString()}
              </div>
            </div>
          </motion.div>

          <motion.div
            whileHover={{ scale: 1.02 }}
            className="card"
          >
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Model Insights</h3>
            <div className="space-y-3">
              <div className="bg-blue-50 rounded-lg p-3">
                <p className="text-sm text-blue-800">
                  <strong>High Confidence:</strong> Model shows strong signals for upward movement
                </p>
              </div>
              <div className="bg-green-50 rounded-lg p-3">
                <p className="text-sm text-green-800">
                  <strong>Recent Training:</strong> Model updated with latest market data
                </p>
              </div>
              <div className="bg-yellow-50 rounded-lg p-3">
                <p className="text-sm text-yellow-800">
                  <strong>Volatility Alert:</strong> Increased market volatility detected
                </p>
              </div>
            </div>
          </motion.div>
        </div>
      </div>

      {/* Data Quality Metrics */}
      <div>
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Data Quality Metrics</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <QualityMetricCard
            metric="Completeness"
            value={analyticsData.dataQuality.completeness}
            color="text-green-500"
          />
          <QualityMetricCard
            metric="Accuracy"
            value={analyticsData.dataQuality.accuracy}
            color="text-blue-500"
          />
          <QualityMetricCard
            metric="Consistency"
            value={analyticsData.dataQuality.consistency}
            color="text-purple-500"
          />
          <QualityMetricCard
            metric="Timeliness"
            value={analyticsData.dataQuality.timeliness}
            color="text-orange-500"
          />
        </div>
      </div>

      {/* Quick Actions */}
      <div className="card">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Analytics Actions</h2>
        <div className="flex space-x-4">
          <button className="btn-primary">
            Refresh Analytics
          </button>
          <button className="btn-secondary">
            Export Report
          </button>
          <button className="btn-secondary">
            View Historical Data
          </button>
        </div>
      </div>
    </div>
  )
}
