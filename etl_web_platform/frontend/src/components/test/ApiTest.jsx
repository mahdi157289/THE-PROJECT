import React from 'react';
import { useApiData, useApiConnection } from '../../hooks/useApiData';
import { getETLStatus, getSystemStats, getScrapingStatus, checkHealth } from '../../services/api';
import { LoadingSpinner, LoadingSkeleton } from '../ui/LoadingSpinner';
import { ErrorMessage, ConnectionError } from '../ui/ErrorMessage';

/**
 * Test Component to verify API Service Foundation
 * This will test all our new components and hooks
 */
export function ApiTest() {
  // Test API connectivity
  const { isConnected, testConnection } = useApiConnection();
  
  // Test data fetching with different intervals
  const etlStatus = useApiData(getETLStatus, 5000); // 5 second refresh
  const systemStats = useApiData(getSystemStats, 10000); // 10 second refresh
  const scrapingStatus = useApiData(getScrapingStatus, 15000); // 15 second refresh
  const healthCheck = useApiData(checkHealth, 30000); // 30 second refresh

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">🧪 API Service Foundation Test</h1>
      
      {/* Connection Status */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">🔗 Connection Status</h2>
        <div className="flex items-center space-x-3">
          <div className={`w-3 h-3 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`} />
          <span className={isConnected ? 'text-green-700' : 'text-red-700'}>
            {isConnected ? 'Connected to Backend' : 'Disconnected from Backend'}
          </span>
          <button
            onClick={testConnection}
            className="px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600"
          >
            Test Connection
          </button>
        </div>
      </div>

      {/* ETL Status Test */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">📊 ETL Status Test (5s refresh)</h2>
        {etlStatus.loading && <LoadingSpinner text="Loading ETL Status..." />}
        {etlStatus.error && (
          <ErrorMessage 
            error={etlStatus.error} 
            onRetry={etlStatus.refresh}
            title="ETL Status Error"
          />
        )}
        {etlStatus.data && (
          <div className="space-y-2">
            <p className="text-sm text-gray-600">✅ Data loaded successfully</p>
            <pre className="bg-gray-100 p-2 rounded text-xs overflow-auto">
              {JSON.stringify(etlStatus.data, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* System Stats Test */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">💻 System Stats Test (10s refresh)</h2>
        {systemStats.loading && <LoadingSpinner text="Loading System Stats..." />}
        {systemStats.error && (
          <ErrorMessage 
            error={systemStats.error} 
            onRetry={systemStats.refresh}
            title="System Stats Error"
          />
        )}
        {systemStats.data && (
          <div className="space-y-2">
            <p className="text-sm text-gray-600">✅ Data loaded successfully</p>
            <pre className="bg-gray-100 p-2 rounded text-xs overflow-auto">
              {JSON.stringify(systemStats.data, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* Scraping Status Test */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">🕷️ Scraping Status Test (15s refresh)</h2>
        {scrapingStatus.loading && <LoadingSpinner text="Loading Scraping Status..." />}
        {scrapingStatus.error && (
          <ErrorMessage 
            error={scrapingStatus.error} 
            onRetry={scrapingStatus.refresh}
            title="Scraping Status Error"
          />
        )}
        {scrapingStatus.data && (
          <div className="space-y-2">
            <p className="text-sm text-gray-600">✅ Data loaded successfully</p>
            <pre className="bg-gray-100 p-2 rounded text-xs overflow-auto">
              {JSON.stringify(scrapingStatus.data, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* Health Check Test */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">🏥 Health Check Test (30s refresh)</h2>
        {healthCheck.loading && <LoadingSpinner text="Loading Health Check..." />}
        {healthCheck.error && (
          <ErrorMessage 
            error={healthCheck.error} 
            onRetry={healthCheck.refresh}
            title="Health Check Error"
          />
        )}
        {healthCheck.data && (
          <div className="space-y-2">
            <p className="text-sm text-gray-600">✅ Data loaded successfully</p>
            <pre className="bg-gray-100 p-2 rounded text-xs overflow-auto">
              {JSON.stringify(healthCheck.data, null, 2)}
            </pre>
          </div>
        )}
      </div>

      {/* Loading Skeleton Test */}
      <div className="bg-white rounded-lg shadow p-4">
        <h2 className="text-lg font-semibold mb-3">🦴 Loading Skeleton Test</h2>
        <LoadingSkeleton rows={4} />
      </div>
    </div>
  );
}
