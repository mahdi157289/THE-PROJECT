import React, { useState } from 'react';
// import { HiPlay, HiStop, HiRefresh, HiDownload, HiDatabase } from 'react-icons/hi';

export function ScrapingLauncher() {
  const [isRunning, setIsRunning] = useState(false);
  const [lastRun, setLastRun] = useState(null);
  const [progress, setProgress] = useState(0);

  const handleStartScraping = async () => {
    setIsRunning(true);
    setProgress(0);
    
    // Simulate progress updates (will be replaced with real API calls)
    const interval = setInterval(() => {
      setProgress(prev => {
        if (prev >= 100) {
          clearInterval(interval);
          setIsRunning(false);
          setLastRun(new Date());
          return 100;
        }
        return prev + Math.random() * 10;
      });
    }, 1000);
  };

  const handleStopScraping = () => {
    setIsRunning(false);
    setProgress(0);
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6 border border-gray-200">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-900 flex items-center">
          <span className="mr-2 text-blue-600">📊</span>
          BVMT Data Scraper
        </h3>
        <div className="flex space-x-2">
          {!isRunning ? (
            <button
              onClick={handleStartScraping}
              className="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg flex items-center transition-colors"
            >
              <span className="mr-2">▶️</span>
              Start Scraping
            </button>
          ) : (
            <button
              onClick={handleStopScraping}
              className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg flex items-center transition-colors"
            >
              <span className="mr-2">⏹️</span>
              Stop Scraping
            </button>
          )}
          <button
            onClick={() => window.location.reload()}
            className="bg-gray-600 hover:bg-gray-700 text-white px-3 py-2 rounded-lg transition-colors"
            title="Refresh Status"
          >
            🔄
          </button>
        </div>
      </div>

      {/* Progress Bar */}
      {isRunning && (
        <div className="mb-4">
          <div className="flex justify-between text-sm text-gray-600 mb-2">
            <span>Scraping Progress</span>
            <span>{Math.round(progress)}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div 
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${progress}%` }}
            ></div>
          </div>
        </div>
      )}

      {/* Status Info */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="text-center p-3 bg-gray-50 rounded-lg">
          <div className="text-2xl font-bold text-blue-600">
            {isRunning ? '🔄' : '✅'}
          </div>
          <div className="text-sm text-gray-600">
            {isRunning ? 'Running' : 'Ready'}
          </div>
        </div>
        
        <div className="text-center p-3 bg-gray-50 rounded-lg">
          <div className="text-2xl font-bold text-green-600">
            📥
          </div>
          <div className="text-sm text-gray-600">
            {lastRun ? 'Last Run: ' + lastRun.toLocaleTimeString() : 'Never Run'}
          </div>
        </div>
        
        <div className="text-center p-3 bg-gray-50 rounded-lg">
          <div className="text-2xl font-bold text-purple-600">
            17
          </div>
          <div className="text-sm text-gray-600">
            Years Available (2008-2024)
          </div>
        </div>
      </div>
    </div>
  );
}
