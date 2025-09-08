import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import FileNotificationSystem from '../components/scraping/FileNotificationSystem';
import CSVFileCards from '../components/scraping/CSVFileCards';
import DynamicProgressTracker from '../components/scraping/DynamicProgressTracker';

const containerVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1
    }
  }
};

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.5,
      ease: "easeOut"
    }
  }
};

const cardVariants = {
  hidden: { opacity: 0, scale: 0.9 },
  visible: {
    opacity: 1,
    scale: 1,
    transition: {
      duration: 0.3,
      ease: "easeOut"
    }
  }
};

export default function ScrapingMonitor() {
  const [isRunning, setIsRunning] = useState(false);
  const [status, setStatus] = useState('');
  const [scrapingStatus, setScrapingStatus] = useState(null);
  const [logs, setLogs] = useState([]);
  const [files, setFiles] = useState([]);

  // Fetch initial logs and status
  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const [statusResponse, logsResponse, filesResponse] = await Promise.all([
          fetch('http://127.0.0.1:5000/api/scraping/status'),
          fetch('http://127.0.0.1:5000/api/scraping/logs'),
          fetch('http://127.0.0.1:5000/api/scraping/files')
        ]);

        const statusData = await statusResponse.json();
        const logsData = await logsResponse.json();
        const filesData = await filesResponse.json();

        if (statusData.status === 'success') {
          setScrapingStatus(statusData.data);
          setIsRunning(statusData.data.is_running);
        }

        if (logsData.status === 'success' && logsData.data.logs) {
          setLogs(logsData.data.logs);
        }

        if (filesData.status === 'success' && filesData.data.files) {
          setFiles(filesData.data.files);
        }
      } catch (error) {
        console.log('Initial data fetch failed:', error);
      }
    };

    fetchInitialData();
  }, []);

  // Poll backend for real-time status
  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const [statusResponse, filesResponse] = await Promise.all([
          fetch('http://127.0.0.1:5000/api/scraping/status'),
          fetch('http://127.0.0.1:5000/api/scraping/files')
        ]);

        const statusData = await statusResponse.json();
        const filesData = await filesResponse.json();

        if (statusData.status === 'success') {
          setScrapingStatus(statusData.data);

          // Update logs
          if (statusData.data.logs && statusData.data.logs.length > 0) {
            setLogs(statusData.data.logs);
          }

          // Check if scraping finished
          if (!statusData.data.is_running && isRunning) {
            setIsRunning(false);
            setStatus('✅ Scraping completed!');
          }
        }

        if (filesData.status === 'success' && filesData.data.files) {
          setFiles(filesData.data.files);
        }
      } catch (error) {
        console.log('Status check failed:', error);
      }
    }, 2000); // Check every 2 seconds for more responsive updates

    return () => clearInterval(interval);
  }, [isRunning]);

  const handleStartScraping = async () => {
    console.log('🔵 Button clicked! Starting scraping...');
    setIsRunning(true);
    setStatus('Starting scraper...');
    setLogs([]);
    setScrapingStatus(null);
    
    try {
      console.log('🔵 Making API call to backend...');
      const response = await fetch('http://127.0.0.1:5000/api/scraping/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      console.log('🔵 Response received:', response);
      const data = await response.json();
      console.log('🔵 Data received:', data);
      
      if (data.status === 'success') {
        setStatus('✅ Scraping started successfully! Check progress below...');
      } else {
        setStatus('❌ Error: ' + data.message);
        setIsRunning(false);
      }
    } catch (error) {
      console.error('🔴 Error occurred:', error);
      setStatus('❌ Connection error: ' + error.message);
      setIsRunning(false);
    }
  };

  const handleStopScraping = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/scraping/stop', {
        method: 'POST',
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        setStatus('⏹️ Scraping stopped by user');
        setIsRunning(false);
      }
    } catch (error) {
      setStatus('❌ Error stopping scraper: ' + error.message);
    }
  };

  const testBackendConnection = async () => {
    console.log('🧪 Testing backend connection...');
    try {
      const response = await fetch('http://127.0.0.1:5000/');
      const text = await response.text();
      console.log('🧪 Backend response:', text);
      setStatus('🧪 Backend test: ' + text.substring(0, 50) + '...');
    } catch (error) {
      console.error('🧪 Backend test failed:', error);
      setStatus('🧪 Backend test failed: ' + error.message);
    }
  };

  // Fetch files for progress tracking
  const fetchFiles = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/scraping/files');
      const data = await response.json();
      
      if (data.status === 'success') {
        setFiles(data.data.files);
        console.log(`📁 [ScrapingMonitor] Loaded ${data.data.files.length} files for progress tracking`);
      }
    } catch (error) {
      console.error('❌ [ScrapingMonitor] Failed to fetch files:', error);
    }
  };

  // Fetch files when component mounts and when scraping starts
  useEffect(() => {
    fetchFiles();
    
    // Set up interval to fetch files every 3 seconds for real-time updates
    const interval = setInterval(fetchFiles, 3000);
    
    return () => clearInterval(interval);
  }, []);

  return (
    <motion.div 
      className="space-y-6"
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {/* Header */}
      <motion.div variants={itemVariants}>
        <h1 className="text-4xl font-serif font-bold text-pure-white mb-2 text-center">Scraping Monitor</h1>
        <p className="text-lg text-crystal-white font-sans text-center">
          Monitor and control BVMT data scraping operations in real-time
        </p>
        
        {/* Long Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>
      </motion.div>

      {/* Control Panel */}
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01 }}
      >
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-2xl font-serif font-semibold text-pure-white">Scraping Control</h2>
            <p className="text-crystal-white font-sans">Manage BVMT data collection</p>
          </div>
          <div className="flex items-center space-x-4">
            <motion.button
              onClick={testBackendConnection}
              className="px-4 py-2 bg-crystal-white hover:bg-secondary-600 text-dark-bg font-display font-medium rounded-lg transition-all duration-300"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              Test Connection
            </motion.button>
            <motion.button
              onClick={handleStartScraping}
              disabled={isRunning}
              className="px-6 py-2 bg-light-green hover:bg-primary-600 disabled:bg-pearl-white disabled:opacity-50 text-dark-bg font-display font-medium rounded-lg transition-all duration-300 disabled:cursor-not-allowed"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {isRunning ? 'Running...' : 'Start Scraping'}
            </motion.button>
            <motion.button
              onClick={handleStopScraping}
              disabled={!isRunning}
              className="px-6 py-2 bg-red-400 hover:bg-red-500 disabled:bg-pearl-white disabled:opacity-50 text-dark-bg font-display font-medium rounded-lg transition-all duration-300 disabled:cursor-not-allowed"
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              Stop Scraping
            </motion.button>
          </div>
        </div>

        {/* Status Display */}
        <motion.div 
          className="p-4 bg-background-tertiary rounded-lg border border-light-silver"
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
        >
          <div className="flex items-center space-x-3">
            <motion.div
              className={`w-3 h-3 rounded-full ${isRunning ? 'bg-light-green animate-pulse' : 'bg-pearl-white'}`}
              animate={{ scale: isRunning ? [1, 1.2, 1] : 1 }}
              transition={{ duration: 1, repeat: isRunning ? Infinity : 0 }}
            />
            <span className="text-crystal-white font-sans">
              {status || 'Ready to start scraping...'}
            </span>
          </div>
        </motion.div>
      </motion.div>

      {/* Progress Tracker */}
      <motion.div variants={cardVariants}>
        <DynamicProgressTracker 
          files={files}
          isRunning={isRunning} 
          scrapingStatus={scrapingStatus}
        />
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-6"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* File Notifications */}
      <motion.div variants={cardVariants}>
        <FileNotificationSystem 
          files={files}
          isRunning={isRunning}
        />
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-6"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* CSV File Cards */}
      <motion.div variants={cardVariants}>
        <CSVFileCards 
          files={files}
          isRunning={isRunning}
        />
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-6"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* Logs Section */}
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01 }}
      >
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-xl font-serif font-semibold text-pure-white">Scraping Logs</h3>
          <motion.button
            onClick={async () => {
              try {
                const response = await fetch('http://127.0.0.1:5000/api/scraping/logs');
                const data = await response.json();
                if (data.status === 'success' && data.data.logs) {
                  setLogs(data.data.logs);
                }
              } catch (error) {
                console.log('Failed to refresh logs:', error);
              }
            }}
            className="px-3 py-1 bg-light-green hover:bg-primary-600 text-dark-bg font-display font-medium rounded-lg transition-all duration-300 text-sm"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            Refresh Logs
          </motion.button>
        </div>
        <div className="bg-background-primary rounded-lg p-4 max-h-64 overflow-y-auto custom-scrollbar">
          {logs.length > 0 ? (
            <div className="space-y-1">
              {logs.map((log, index) => (
                <motion.div
                  key={index}
                  className="text-sm font-mono text-crystal-white p-1"
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: index * 0.05 }}
                >
                  {log.message || log}
                </motion.div>
              ))}
            </div>
          ) : (
            <p className="text-pearl-white font-sans">No logs available. Start scraping to see activity.</p>
          )}
        </div>
      </motion.div>
    </motion.div>
  );
}
