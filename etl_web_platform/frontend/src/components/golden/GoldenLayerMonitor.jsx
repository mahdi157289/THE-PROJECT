import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

const GoldenLayerMonitor = () => {
  const [goldenStatus, setGoldenStatus] = useState({
    status: 'Idle',
    dataStats: {
      qualityScore: 0,
      inputRows: {
        cotations: 0,
        indices: 0
      },
      outputRows: {
        cotations: 0,
        indices: 0
      },
      transformationStats: {
        loadedRows: 0,
        processedRows: 0,
        validatedRows: 0
      }
    },
    performance: {
      totalDuration: 0,
      throughput: 0
    },
    lastRun: null
  });

  const [isRunning, setIsRunning] = useState(false);
  const [expandedFeatures, setExpandedFeatures] = useState(new Set());

  // Animation variants
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
        duration: 0.5
    }
  }
};

const cardVariants = {
    hidden: { opacity: 0, scale: 0.95 },
  visible: {
    opacity: 1,
    scale: 1,
    transition: {
        duration: 0.5,
      ease: "easeOut"
    }
  }
};

  // Fetch golden layer status
  const fetchGoldenStatus = async () => {
      try {
        const response = await fetch('http://127.0.0.1:5000/api/golden/status');
      if (response.ok) {
        const data = await response.json();
        setGoldenStatus(data);
        }
      } catch (error) {
        console.error('Failed to fetch golden status:', error);
      }
    };

  // Start golden layer
  const startGoldenLayer = async () => {
    try {
      setIsRunning(true);
      const response = await fetch('http://127.0.0.1:5000/api/golden/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (response.ok) {
      const data = await response.json();
        console.log('Golden layer started successfully');
        await fetchGoldenStatus();
      } else {
        const data = await response.json();
        throw new Error(data.message || 'Failed to start golden layer');
      }
    } catch (error) {
      console.error('Failed to start golden layer:', error);
    } finally {
      setIsRunning(false);
    }
  };

  // Poll for status updates
  useEffect(() => {
        fetchGoldenStatus();
    const interval = setInterval(fetchGoldenStatus, 2000);
    return () => clearInterval(interval);
  }, []);

  const getStatusColor = (status) => {
    switch (status.toLowerCase()) {
      case 'running':
        return 'bg-light-green text-background-primary';
      case 'completed':
        return 'bg-blue-500 text-white';
      case 'error':
        return 'bg-red-500 text-white';
      default:
        return 'bg-gray-500 text-white';
    }
  };

  const getQualityColor = (score) => {
    if (score >= 90) return 'text-light-green'
    if (score >= 70) return 'text-yellow-400'
    return 'text-red-400'
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
      initial="hidden"
      animate="visible"
    >
      {/* Header */}
      <motion.div variants={itemVariants}>
        <div className="flex items-center justify-center mb-2">
          <span className="text-4xl mr-3">🥇</span>
          <h2 className="text-3xl font-display font-bold text-pure-white">Golden Layer</h2>
        </div>
        <p className="text-lg text-crystal-white font-sans text-center">
          Business logic and transformations layer
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
            <h3 className="text-xl font-display font-semibold text-pure-white text-center">Layer Status</h3>
            <p className="text-crystal-white font-sans text-center">
              {goldenStatus.status}
            </p>
          </div>
        </div>

      {/* Data Quality Score */}
      <motion.div 
        className="mb-6"
        variants={itemVariants}
      >
          <div className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center glass">
          <div className="flex items-center justify-center mb-2">
            <span className="text-sm font-display font-medium text-crystal-white">Data Quality Score</span>
              <span className="text-2xl text-pearl-white ml-2">🎯</span>
          </div>
          <div className="flex items-center justify-center space-x-3">
            <div className={`text-3xl font-display font-bold ${getQualityColor(goldenStatus.dataStats?.qualityScore || 0)}`}>
                {goldenStatus.dataStats?.qualityScore || 0}%
            </div>
            <div className="flex-1 bg-background-primary rounded-full h-3 overflow-hidden max-w-xs">
              <motion.div 
                className="bg-light-green h-3 rounded-full"
                initial={{ width: 0 }}
                animate={{ width: `${goldenStatus.dataStats?.qualityScore || 0}%` }}
                transition={{ duration: 1, ease: "easeOut" }}
              ></motion.div>
            </div>
          </div>
        </div>
      </motion.div>

      {/* Data Statistics */}
      <motion.div 
        className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6"
        variants={containerVariants}
      >
        <motion.div 
            className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center glass"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="flex items-center justify-center mb-2">
            <span className="text-sm font-display font-medium text-crystal-white">Input Data</span>
              <span className="text-2xl text-pearl-white ml-2">📥</span>
          </div>
          <div className="space-y-2">
            <div className="flex justify-center">
              <span className="text-xs text-crystal-white">Cotations</span>
              <span className="text-sm font-display font-medium text-pure-white ml-2">
                {(goldenStatus.dataStats?.inputRows?.cotations || 0).toLocaleString()}
              </span>
            </div>
            <div className="flex justify-center">
              <span className="text-xs text-crystal-white">Indices</span>
              <span className="text-sm font-display font-medium text-pure-white ml-2">
                {(goldenStatus.dataStats?.inputRows?.indices || 0).toLocaleString()}
              </span>
            </div>
          </div>
        </motion.div>

        <motion.div 
            className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center glass"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="flex items-center justify-center mb-2">
            <span className="text-sm font-display font-medium text-crystal-white">Processed Data</span>
              <span className="text-2xl text-pearl-white ml-2">⚙️</span>
          </div>
          <div className="space-y-2">
            <div className="flex justify-center">
              <span className="text-xs text-crystal-white">Loaded</span>
              <span className="text-sm font-display font-medium text-pure-white ml-2">
                {(goldenStatus.dataStats?.transformationStats?.loadedRows || 0).toLocaleString()}
              </span>
            </div>
            <div className="flex justify-center">
              <span className="text-xs text-crystal-white">Validated</span>
              <span className="text-sm font-display font-medium text-pure-white ml-2">
                {(goldenStatus.dataStats?.transformationStats?.validatedRows || 0).toLocaleString()}
              </span>
            </div>
          </div>
        </motion.div>
      </motion.div>

        {/* Performance Metrics */}
      <motion.div 
        className="mb-6"
        variants={itemVariants}
      >
          <h4 className="text-lg font-display font-semibold text-pure-white mb-3 text-center">Performance Metrics</h4>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center glass"
            whileHover={{ scale: 1.02 }}
          >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Total Duration</div>
            <div className="text-lg font-display font-bold text-light-green">
                {goldenStatus.performance?.totalDuration || 0}s
            </div>
          </motion.div>
          <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center glass"
            whileHover={{ scale: 1.02 }}
          >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Throughput</div>
            <div className="text-lg font-display font-bold text-light-green">
                {(goldenStatus.performance?.throughput || 0).toLocaleString()} rows/s
            </div>
          </motion.div>
          <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center glass"
            whileHover={{ scale: 1.02 }}
          >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Last Run</div>
              <div className="text-lg font-display font-bold text-crystal-white">
                {goldenStatus.lastRun ? new Date(goldenStatus.lastRun).toLocaleTimeString() : 'Never'}
            </div>
          </motion.div>
        </div>
      </motion.div>

      {/* Layer Features */}
      <motion.div 
        className="mb-6"
        variants={itemVariants}
      >
        <h4 className="text-lg font-display font-semibold text-pure-white mb-3 text-center">Layer Features</h4>
        <div className="space-y-3">
          {/* Business Logic */}
          <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
            whileHover={{ scale: 1.01 }}
          >
            <motion.button
              onClick={() => toggleFeatureExpansion('businessLogic')}
              className="w-full p-4 text-left flex items-center justify-between"
              whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
              whileTap={{ scale: 0.98 }}
            >
                <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">💼</span>
                <span className="font-display font-semibold text-pure-white">Business Logic</span>
              </div>
              <motion.span
                animate={{ rotate: expandedFeatures.has('businessLogic') ? 180 : 0 }}
                transition={{ duration: 0.2 }}
                className="text-crystal-white"
              >
                ▼
              </motion.span>
            </motion.button>
            <AnimatePresence>
              {expandedFeatures.has('businessLogic') && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                  className="px-4 pb-4"
                >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                    <li>• Financial calculations</li>
                      <li>• Business rules</li>
                      <li>• Data aggregations</li>
                      <li>• KPI computations</li>
                      <li>• Risk assessments</li>
                      <li>• Performance metrics</li>
                  </ul>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>

          {/* Data Aggregation */}
          <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
            whileHover={{ scale: 1.01 }}
          >
            <motion.button
              onClick={() => toggleFeatureExpansion('dataAggregation')}
              className="w-full p-4 text-left flex items-center justify-between"
              whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
              whileTap={{ scale: 0.98 }}
            >
                <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">📊</span>
                <span className="font-display font-semibold text-pure-white">Data Aggregation</span>
              </div>
              <motion.span
                animate={{ rotate: expandedFeatures.has('dataAggregation') ? 180 : 0 }}
                transition={{ duration: 0.2 }}
                className="text-crystal-white"
              >
                ▼
              </motion.span>
            </motion.button>
            <AnimatePresence>
              {expandedFeatures.has('dataAggregation') && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                  className="px-4 pb-4"
                >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                    <li>• Time-based aggregations</li>
                      <li>• Sector-wise grouping</li>
                      <li>• Statistical summaries</li>
                    <li>• Rolling calculations</li>
                  </ul>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>

          {/* Data Marts */}
          <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
            whileHover={{ scale: 1.01 }}
          >
            <motion.button
              onClick={() => toggleFeatureExpansion('dataMarts')}
              className="w-full p-4 text-left flex items-center justify-between"
              whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
              whileTap={{ scale: 0.98 }}
            >
                <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">🏪</span>
                <span className="font-display font-semibold text-pure-white">Data Marts</span>
              </div>
              <motion.span
                animate={{ rotate: expandedFeatures.has('dataMarts') ? 180 : 0 }}
                transition={{ duration: 0.2 }}
                className="text-crystal-white"
              >
                ▼
              </motion.span>
            </motion.button>
            <AnimatePresence>
              {expandedFeatures.has('dataMarts') && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                  className="px-4 pb-4"
                >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                    <li>• Star schema design</li>
                    <li>• Dimension tables</li>
                    <li>• Fact tables</li>
                    <li>• Optimized queries</li>
                  </ul>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        </div>
      </motion.div>

      {/* Action Buttons */}
      <motion.div 
          className="flex justify-center mt-6"
        variants={itemVariants}
      >
        <motion.button
            onClick={startGoldenLayer}
            disabled={isRunning}
                            className="px-24 py-3 bg-light-green text-background-primary font-display font-semibold rounded-lg hover:bg-green-400 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
        >
            {isRunning ? 'Running...' : 'Run Golden Layer'}
        </motion.button>
      </motion.div>
      </motion.div>
    </motion.div>
  );
};

export default GoldenLayerMonitor;
