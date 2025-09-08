import React, { useState, useEffect } from 'react';

const BronzeLayerMonitor = () => {
  const [bronzeStatus, setBronzeStatus] = useState({
    status: 'idle',
    lastRun: null,
    dataStats: {
      cotations: { rows: 0, size: '0 MB', lastUpdate: null },
      indices: { rows: 0, size: '0 MB', lastUpdate: null }
    },
    processing: false,
    logs: []
  });

  const [showDetails, setShowDetails] = useState(false);

  // Fetch initial data from backend
  useEffect(() => {
    fetchBronzeStatus();
    fetchDataStats();
    
    // Poll for updates every 5 seconds
    const interval = setInterval(() => {
      fetchBronzeStatus();
    }, 5000);
    
    return () => clearInterval(interval);
  }, []);

  const fetchBronzeStatus = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/bronze/status');
      const data = await response.json();
      
      if (data.status === 'success') {
        setBronzeStatus(prev => ({
          ...prev,
          status: data.data.status,
          lastRun: data.data.last_run,
          processing: data.data.status === 'running'
        }));
      }
    } catch (error) {
      console.error('Failed to fetch bronze status:', error);
    }
  };

  const fetchDataStats = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/bronze/data-stats');
      const data = await response.json();
      
      if (data.status === 'success') {
        setBronzeStatus(prev => ({
          ...prev,
          dataStats: {
            cotations: { 
              rows: data.data.cotations.rows || 0, 
              size: data.data.cotations.size || '0 MB', 
              lastUpdate: new Date().toISOString() 
            },
            indices: { 
              rows: data.data.indices.rows || 0, 
              size: data.data.indices.size || '0 MB', 
              lastUpdate: new Date().toISOString() 
            }
          }
        }));
      }
    } catch (error) {
      console.error('Failed to fetch data stats:', error);
    }
  };

  const handleRunBronze = async () => {
    setBronzeStatus(prev => ({ ...prev, processing: true, status: 'running' }));
    
    try {
      console.log('🚀 Starting Bronze Layer processing...');
      
      const response = await fetch('http://127.0.0.1:5000/api/bronze/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      const data = await response.json();
      
      if (data.status === 'success') {
        console.log('✅ Bronze pipeline started successfully');
        // The status will be updated by the polling mechanism
      } else {
        throw new Error(data.message || 'Failed to start bronze pipeline');
      }
      
    } catch (error) {
      console.error('❌ Error starting bronze pipeline:', error);
      setBronzeStatus(prev => ({
        ...prev,
        processing: false,
        status: 'error',
        logs: [
          ...prev.logs,
          { timestamp: new Date().toISOString(), message: `❌ Error: ${error.message}`, level: 'error' }
        ]
      }));
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'idle': return 'bg-gray-500';
      case 'running': return 'bg-blue-500';
      case 'completed': return 'bg-green-500';
      case 'error': return 'bg-red-500';
      default: return 'bg-gray-500';
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'idle': return 'Ready';
      case 'running': return 'Processing';
      case 'completed': return 'Completed';
      case 'error': return 'Error';
      default: return 'Unknown';
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center space-x-3">
          <div className="w-12 h-12 bg-yellow-100 rounded-full flex items-center justify-center">
            <span className="text-2xl">🥉</span>
          </div>
          <div>
            <h3 className="text-xl font-semibold text-gray-900">Bronze Layer Monitor</h3>
            <p className="text-sm text-gray-600">Raw data ingestion and preprocessing</p>
          </div>
        </div>
        
        <div className="flex items-center space-x-3">
          <div className={`px-3 py-1 rounded-full text-white text-sm font-medium ${getStatusColor(bronzeStatus.status)}`}>
            {getStatusText(bronzeStatus.status)}
          </div>
          <button
            onClick={() => setShowDetails(!showDetails)}
            className="text-gray-500 hover:text-gray-700"
          >
            {showDetails ? '▼' : '▶'}
          </button>
        </div>
      </div>

      {/* Data Statistics */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        {/* Cotations */}
        <div className="bg-blue-50 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <h4 className="font-medium text-blue-900">Cotations Data</h4>
              <p className="text-2xl font-bold text-blue-700">{bronzeStatus.dataStats.cotations.rows.toLocaleString()}</p>
              <p className="text-sm text-blue-600">{bronzeStatus.dataStats.cotations.size}</p>
            </div>
            <div className="text-blue-400">
              <svg className="w-8 h-8" fill="currentColor" viewBox="0 0 20 20">
                <path d="M3 4a1 1 0 011-1h12a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1V4zM3 10a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H4a1 1 0 01-1-1v-6zM14 9a1 1 0 00-1 1v6a1 1 0 001 1h2a1 1 0 001-1v-6a1 1 0 00-1-1h-2z"/>
              </svg>
            </div>
          </div>
        </div>

        {/* Indices */}
        <div className="bg-green-50 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div>
              <h4 className="font-medium text-green-900">Indices Data</h4>
              <p className="text-2xl font-bold text-green-700">{bronzeStatus.dataStats.indices.rows.toLocaleString()}</p>
              <p className="text-sm text-green-600">{bronzeStatus.dataStats.indices.size}</p>
            </div>
            <div className="text-green-400">
              <svg className="w-8 h-8" fill="currentColor" viewBox="0 0 20 20">
                <path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zM8 7a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zM14 4a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z"/>
              </svg>
            </div>
          </div>
        </div>
      </div>

      {/* Action Button */}
      <div className="mb-6">
        <button
          onClick={handleRunBronze}
          disabled={bronzeStatus.processing}
          className={`w-full py-3 px-4 rounded-lg font-medium text-white transition-colors ${
            bronzeStatus.processing
              ? 'bg-gray-400 cursor-not-allowed'
              : 'bg-yellow-600 hover:bg-yellow-700'
          }`}
        >
          {bronzeStatus.processing ? (
            <div className="flex items-center justify-center space-x-2">
              <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
              <span>Processing Bronze Layer...</span>
            </div>
          ) : (
            '🚀 Run Bronze Layer Processing'
          )}
        </button>
      </div>

      {/* Last Run Info */}
      {bronzeStatus.lastRun && (
        <div className="mb-6 p-4 bg-gray-50 rounded-lg">
          <h4 className="font-medium text-gray-900 mb-2">Last Run</h4>
          <p className="text-sm text-gray-600">
            {new Date(bronzeStatus.lastRun).toLocaleString()}
          </p>
        </div>
      )}

      {/* Detailed Information */}
      {showDetails && (
        <div className="border-t pt-6">
          <h4 className="font-medium text-gray-900 mb-4">Processing Details</h4>
          
          {/* Processing Steps */}
          <div className="space-y-3 mb-6">
            <div className="flex items-center space-x-3">
              <div className="w-6 h-6 bg-green-100 rounded-full flex items-center justify-center">
                <span className="text-green-600 text-sm">✓</span>
              </div>
              <span className="text-sm text-gray-700">Data validation and schema creation</span>
            </div>
            <div className="flex items-center space-x-3">
              <div className="w-6 h-6 bg-green-100 rounded-full flex items-center justify-center">
                <span className="text-green-600 text-sm">✓</span>
              </div>
              <span className="text-sm text-gray-700">Column case analysis and standardization</span>
            </div>
            <div className="flex items-center space-x-3">
              <div className="w-6 h-6 bg-green-100 rounded-full flex items-center justify-center">
                <span className="text-green-600 text-sm">✓</span>
              </div>
              <span className="text-sm text-gray-700">Data type conversion and preprocessing</span>
            </div>
            <div className="flex items-center space-x-3">
              <div className="w-6 h-6 bg-green-100 rounded-full flex items-center justify-center">
                <span className="text-green-600 text-sm">✓</span>
              </div>
              <span className="text-sm text-gray-700">Parquet file generation</span>
            </div>
            <div className="flex items-center space-x-3">
              <div className="w-6 h-6 bg-green-100 rounded-full flex items-center justify-center">
                <span className="text-green-600 text-sm">✓</span>
              </div>
              <span className="text-sm text-gray-700">PostgreSQL database storage</span>
            </div>
          </div>

          {/* Recent Logs */}
          {bronzeStatus.logs.length > 0 && (
            <div>
              <h5 className="font-medium text-gray-900 mb-3">Recent Logs</h5>
              <div className="space-y-2 max-h-40 overflow-y-auto">
                {bronzeStatus.logs.slice(-5).map((log, index) => (
                  <div key={index} className="flex items-start space-x-2 text-sm">
                    <span className="text-gray-500 text-xs">
                      {new Date(log.timestamp).toLocaleTimeString()}
                    </span>
                    <span className={`${
                      log.level === 'error' ? 'text-red-600' : 'text-gray-700'
                    }`}>
                      {log.message}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default BronzeLayerMonitor;
