import React, { useState, useEffect } from 'react';
// import { HiClock, HiCheckCircle, HiExclamationTriangle, HiXCircle, HiChartBar, HiDatabase } from 'react-icons/hi';

export function ScrapingMonitorDashboard() {
  const [logs, setLogs] = useState([]);
  const [stats, setStats] = useState({
    totalYears: 17,
    successfulYears: 0,
    failedYears: 0,
    currentYear: null,
    currentProgress: 0,
    estimatedTime: '0:00'
  });

  // Simulate real-time log updates (will be replaced with WebSocket/API calls)
  useEffect(() => {
    const mockLogs = [
      { timestamp: new Date(), level: 'INFO', message: 'Starting BVMT data processing...', module: 'main' },
      { timestamp: new Date(Date.now() - 1000), level: 'INFO', message: 'Processing indices for year 2008...', module: 'process_year' },
      { timestamp: new Date(Date.now() - 2000), level: 'INFO', message: 'Downloading histo_indice_2008.rar...', module: 'download_file' },
      { timestamp: new Date(Date.now() - 3000), level: 'SUCCESS', message: 'Successfully extracted 2008 indices data', module: 'extract_data' },
      { timestamp: new Date(Date.now() - 4000), level: 'INFO', message: 'Processing indices for year 2009...', module: 'process_year' },
      { timestamp: new Date(Date.now() - 5000), level: 'WARNING', message: 'File histo_indice_2009.rar not found, trying alternate source', module: 'try_alternate_source' },
      { timestamp: new Date(Date.now() - 6000), level: 'SUCCESS', message: 'Retrieved 2009 data from alternate source', module: 'try_alternate_source' },
    ];

    setLogs(mockLogs);

    // Simulate real-time updates
    const interval = setInterval(() => {
      const newLog = {
        timestamp: new Date(),
        level: ['INFO', 'SUCCESS', 'WARNING', 'ERROR'][Math.floor(Math.random() * 4)],
        message: 'Processing data...',
        module: 'scraper'
      };
      setLogs(prev => [newLog, ...prev.slice(0, 19)]); // Keep last 20 logs
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  const getLogIcon = (level) => {
    switch (level) {
      case 'SUCCESS': return <span className="text-green-500">✅</span>;
      case 'WARNING': return <span className="text-yellow-500">⚠️</span>;
      case 'ERROR': return <span className="text-red-500">❌</span>;
      default: return <span className="text-blue-500">🕐</span>;
    }
  };

  const getLogColor = (level) => {
    switch (level) {
      case 'SUCCESS': return 'bg-green-50 border-green-200';
      case 'WARNING': return 'bg-yellow-50 border-yellow-200';
      case 'ERROR': return 'bg-red-50 border-red-200';
      default: return 'bg-blue-50 border-blue-200';
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">BVMT Scraping Monitor</h1>
          <p className="mt-2 text-gray-600">Real-time monitoring of BVMT data scraping operations</p>
        </div>
        <div className="flex items-center space-x-2">
          <div className="px-3 py-1 bg-green-100 text-green-800 rounded-full text-sm font-medium">
            Active
          </div>
          <div className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-medium">
            Live Logs
          </div>
        </div>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-blue-100 rounded-lg">
              <span className="h-6 w-6 text-blue-600">📊</span>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Total Years</p>
              <p className="text-2xl font-bold text-gray-900">{stats.totalYears}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-green-100 rounded-lg">
              <span className="h-6 w-6 text-green-600">✅</span>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Successful</p>
              <p className="text-2xl font-bold text-green-600">{stats.successfulYears}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-red-100 rounded-lg">
              <span className="h-6 w-6 text-red-600">❌</span>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Failed</p>
              <p className="text-2xl font-bold text-red-600">{stats.failedYears}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <div className="flex items-center">
            <div className="p-2 bg-purple-100 rounded-lg">
              <span className="h-6 w-6 text-purple-600">📈</span>
            </div>
            <div className="ml-4">
              <p className="text-sm font-medium text-gray-600">Current Year</p>
              <p className="text-2xl font-bold text-purple-600">
                {stats.currentYear || 'N/A'}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Progress Section */}
      {stats.currentYear && (
        <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Current Progress</h3>
          <div className="space-y-3">
            <div className="flex justify-between text-sm text-gray-600">
              <span>Processing year {stats.currentYear}</span>
              <span>{stats.currentProgress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-3">
              <div 
                className="bg-blue-600 h-3 rounded-full transition-all duration-300"
                style={{ width: `${stats.currentProgress}%` }}
              ></div>
            </div>
            <div className="text-sm text-gray-500">
              Estimated time remaining: {stats.estimatedTime}
            </div>
          </div>
        </div>
      )}

      {/* Live Logs */}
      <div className="bg-white rounded-lg shadow-md border border-gray-200">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-semibold text-gray-900">Live Logs</h3>
          <p className="text-sm text-gray-600">Real-time scraping operation logs</p>
        </div>
        <div className="p-6">
          <div className="space-y-3 max-h-96 overflow-y-auto">
            {logs.map((log, index) => (
              <div 
                key={index}
                className={`p-3 rounded-lg border ${getLogColor(log.level)}`}
              >
                <div className="flex items-start space-x-3">
                  <div className="flex-shrink-0 mt-1">
                    {getLogIcon(log.level)}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between">
                      <p className="text-sm font-medium text-gray-900">
                        {log.message}
                      </p>
                      <span className="text-xs text-gray-500">
                        {log.timestamp.toLocaleTimeString()}
                      </span>
                    </div>
                    <div className="mt-1 flex items-center space-x-2">
                      <span className="text-xs text-gray-600">
                        Module: {log.module}
                      </span>
                      <span className={`text-xs px-2 py-1 rounded-full ${
                        log.level === 'SUCCESS' ? 'bg-green-100 text-green-800' :
                        log.level === 'WARNING' ? 'bg-yellow-100 text-yellow-800' :
                        log.level === 'ERROR' ? 'bg-red-100 text-red-800' :
                        'bg-blue-100 text-blue-800'
                      }`}>
                        {log.level}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
