import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

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

export default function EnhancedBronzeLayer() {
  const [bronzeStatus, setBronzeStatus] = useState({
    processing: false,
    lastRun: null,
    dataStats: {
      cotations: { rows: 0, size: '0 MB', lastUpdate: null },
      indices: { rows: 0, size: '0 MB', lastUpdate: null }
    },
    performance: {
      totalDuration: 0,
      throughput: 0
    },
    logs: []
  });
  const [showDetails, setShowDetails] = useState(false);
  const [expandedFeatures, setExpandedFeatures] = useState(new Set());
  const [isRunning, setIsRunning] = useState(false);

  // Fetch bronze layer status
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const response = await fetch('http://127.0.0.1:5000/api/bronze/status');
        const data = await response.json();
        
        if (data.status === 'success') {
          console.log('Bronze status response:', data.data); // Debug log
          const backendData = data.data;
          
          // If backend is not processing, reset local running state
          if (!backendData.processing && isRunning) {
            setIsRunning(false);
          }
          
          setBronzeStatus(backendData);
        }
      } catch (error) {
        console.error('Failed to fetch bronze status:', error);
      }
    };

    fetchStatus();
    const interval = setInterval(fetchStatus, 2000); // Poll more frequently
    return () => clearInterval(interval);
  }, []);

  const handleRunBronze = async () => {
    setIsRunning(true);
    try {
      const response = await fetch('http://127.0.0.1:5000/api/bronze/run', {
        method: 'POST'
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        console.log('Bronze layer started successfully');
      } else {
        throw new Error(data.message || 'Failed to start bronze layer');
      }
    } catch (error) {
      console.error('Failed to start bronze layer:', error);
      setIsRunning(false);
    }
  };

  const toggleFeatureExpansion = (featureName) => {
    setExpandedFeatures(prev => {
      const newSet = new Set(prev);
      if (newSet.has(featureName)) {
        newSet.delete(featureName);
      } else {
        newSet.add(featureName);
      }
      return newSet;
    });
  };

  return (
    <motion.div 
      className="space-y-6"
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {/* Header */}
      <motion.div variants={itemVariants}>
        <div className="flex items-center justify-center mb-2">
          <span className="text-4xl mr-3">🥉</span>
          <h2 className="text-3xl font-roboto font-bold text-pure-white">Bronze Layer</h2>
        </div>
        <p className="text-lg text-crystal-white font-sans text-center">
          Raw data ingestion and storage layer
        </p>
      </motion.div>

      {/* Status Card */}
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.1)" }}
      >
        <div className="mb-6">
          <div className="mb-4">
            <h3 className="text-xl font-roboto font-semibold text-pure-white text-center">Layer Status</h3>
            <p className="text-crystal-white font-sans text-center">
              {bronzeStatus.processing ? 'Processing...' : 'Ready to start'}
            </p>
          </div>
          
          {/* Loading Indicator */}
          {(bronzeStatus.processing || isRunning) && (
            <motion.div 
              className="mb-4"
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3 }}
            >
              <div className="flex justify-between text-sm text-crystal-white mb-2">
                <span>Status</span>
                <span>Running...</span>
              </div>
              <div className="w-full bg-background-primary rounded-full h-3 overflow-hidden">
                <motion.div 
                  className="bg-light-green h-3 rounded-full"
                  animate={{ 
                    width: ['0%', '100%', '0%'],
                    x: ['0%', '100%', '0%']
                  }}
                  transition={{ 
                    duration: 2,
                    repeat: Infinity,
                    ease: "easeInOut"
                  }}
                ></motion.div>
              </div>
            </motion.div>
          )}
        </div>

        {/* Data Statistics */}
        <motion.div 
          className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6"
          variants={containerVariants}
        >
          <motion.div 
            className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
            variants={itemVariants}
            whileHover={{ scale: 1.02 }}
          >
            <div className="flex items-center justify-center mb-2">
              <span className="text-sm font-display font-medium text-crystal-white">Cotations</span>
              <span className="text-2xl text-pearl-white ml-2">📈</span>
            </div>
            <div className="text-2xl font-display font-bold text-pure-white">
              {(bronzeStatus.dataStats?.cotations?.rows || 0).toLocaleString()}
            </div>
            <div className="text-xs text-crystal-white font-sans">
              {bronzeStatus.dataStats?.cotations?.size || '0 MB'} • Last: {bronzeStatus.dataStats?.cotations?.lastUpdate ? new Date(bronzeStatus.dataStats.cotations.lastUpdate).toLocaleTimeString() : 'Never'}
            </div>
          </motion.div>

          <motion.div 
            className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
            variants={itemVariants}
            whileHover={{ scale: 1.02 }}
          >
            <div className="flex items-center justify-center mb-2">
              <span className="text-sm font-display font-medium text-crystal-white">Indices</span>
              <span className="text-2xl text-pearl-white ml-2">📊</span>
            </div>
            <div className="text-2xl font-display font-bold text-pure-white">
              {(bronzeStatus.dataStats?.indices?.rows || 0).toLocaleString()}
            </div>
            <div className="text-xs text-crystal-white font-sans">
              {bronzeStatus.dataStats?.indices?.size || '0 MB'} • Last: {bronzeStatus.dataStats?.indices?.lastUpdate ? new Date(bronzeStatus.dataStats.indices.lastUpdate).toLocaleTimeString() : 'Never'}
            </div>
          </motion.div>
        </motion.div>

        {/* Performance Metrics */}
        <motion.div 
          className="mb-6"
          variants={itemVariants}
        >
          <h4 className="text-lg font-roboto font-semibold text-pure-white mb-3 text-center">Performance Metrics</h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Total Duration</div>
              <div className="text-lg font-display font-bold text-light-green">
                {bronzeStatus.performance?.totalDuration || 0}s
              </div>
            </motion.div>
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Throughput</div>
              <div className="text-lg font-display font-bold text-light-green">
                {(bronzeStatus.performance?.throughput || 0).toLocaleString()} rows/s
              </div>
            </motion.div>
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Last Run</div>
              <div className="text-lg font-display font-bold text-crystal-white">
                {bronzeStatus.lastRun ? new Date(bronzeStatus.lastRun).toLocaleTimeString() : 'Never'}
              </div>
            </motion.div>
          </div>
        </motion.div>

        {/* Layer Features */}
        <motion.div 
          className="mb-6"
          variants={itemVariants}
        >
          <h4 className="text-lg font-roboto font-semibold text-pure-white mb-3 text-center">Layer Features</h4>
          <div className="space-y-3">
            {/* Data Ingestion */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('dataIngestion')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
              <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">📥</span>
                <span className="font-roboto font-semibold text-pure-white">Data Ingestion</span>
              </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('dataIngestion') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('dataIngestion') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• Automated CSV file processing</li>
                      <li>• ZIP/RAR archive extraction</li>
                      <li>• Real-time data validation</li>
                      <li>• Incremental data loading</li>
                    </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Data Storage */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('dataStorage')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
              <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">💾</span>
                <span className="font-roboto font-semibold text-pure-white">Data Storage</span>
              </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('dataStorage') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('dataStorage') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• PostgreSQL database storage</li>
                      <li>• Optimized table partitioning</li>
                      <li>• Data compression techniques</li>
                      <li>• Backup and recovery systems</li>
                    </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Data Quality */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('dataQuality')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
              <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">🔍</span>
                <span className="font-roboto font-semibold text-pure-white">Data Quality</span>
              </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('dataQuality') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('dataQuality') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• Schema validation</li>
                      <li>• Data type checking</li>
                      <li>• Null value handling</li>
                      <li>• Duplicate detection</li>
                    </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          </div>
        </motion.div>

        {/* Action Button */}
        <motion.div 
          className="flex justify-center mt-6"
          variants={itemVariants}
        >
          <motion.button
            onClick={handleRunBronze}
            disabled={bronzeStatus.processing || isRunning}
                            className="px-24 py-3 bg-light-green hover:bg-primary-600 disabled:bg-pearl-white disabled:opacity-50 text-dark-bg font-display font-medium rounded-lg transition-all duration-300 disabled:cursor-not-allowed"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            {(bronzeStatus.processing || isRunning) ? (
              <motion.div
                className="flex items-center"
                animate={{ rotate: 360 }}
                transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
              >
                <div className="w-5 h-5 border-2 border-dark-bg border-t-transparent rounded-full mr-2"></div>
                Processing...
              </motion.div>
            ) : (
              'Run Bronze Layer'
            )}
          </motion.button>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}
