import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

const SilverLayerMonitor = () => {
  const [silverStatus, setSilverStatus] = useState({
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

  // Fetch silver layer status
  const fetchSilverStatus = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/silver/status');
      if (response.ok) {
        const data = await response.json();
        setSilverStatus(data);
      }
    } catch (error) {
      console.error('Failed to fetch silver status:', error);
    }
  };

  // Start silver layer
  const startSilverLayer = async () => {
    try {
      setIsRunning(true);
      const response = await fetch('http://127.0.0.1:5000/api/silver/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      if (response.ok) {
        const data = await response.json();
        console.log('Silver layer started successfully');
        await fetchSilverStatus();
      } else {
        const data = await response.json();
        throw new Error(data.message || 'Failed to start silver layer');
      }
    } catch (error) {
      console.error('Failed to start silver layer:', error);
    } finally {
      setIsRunning(false);
    }
  };

  // Poll for status updates
  useEffect(() => {
    fetchSilverStatus();
    const interval = setInterval(fetchSilverStatus, 2000);
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
          <span className="text-4xl mr-3">🥈</span>
          <h2 className="text-3xl font-display font-bold text-pure-white">Silver Layer</h2>
        </div>
        <p className="text-lg text-crystal-white font-sans text-center">
          Data cleaning, transformation, and feature engineering layer
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
              {silverStatus.status}
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
                          <div className={`text-3xl font-display font-bold ${getQualityColor(silverStatus.dataStats?.qualityScore || 0)}`}>
              {silverStatus.dataStats?.qualityScore || 0}%
            </div>
              <div className="flex-1 bg-background-primary rounded-full h-3 overflow-hidden max-w-xs">
                <motion.div 
                  className="bg-light-green h-3 rounded-full"
                  initial={{ width: 0 }}
                  animate={{ width: `${silverStatus.dataStats?.qualityScore || 0}%` }}
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
                  {(silverStatus.dataStats?.inputRows?.cotations || 0).toLocaleString()}
                </span>
              </div>
              <div className="flex justify-center">
                <span className="text-xs text-crystal-white">Indices</span>
                <span className="text-sm font-display font-medium text-pure-white ml-2">
                  {(silverStatus.dataStats?.inputRows?.indices || 0).toLocaleString()}
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
              <span className="text-sm font-display font-medium text-crystal-white">Output Data</span>
              <span className="text-2xl text-pearl-white ml-2">📤</span>
            </div>
            <div className="space-y-2">
              <div className="flex justify-center">
                <span className="text-xs text-crystal-white">Cotations</span>
                <span className="text-sm font-display font-medium text-pure-white ml-2">
                  {(silverStatus.dataStats?.outputRows?.cotations || 0).toLocaleString()}
                </span>
              </div>
              <div className="flex justify-center">
                <span className="text-xs text-crystal-white">Indices</span>
                <span className="text-sm font-display font-medium text-pure-white ml-2">
                  {(silverStatus.dataStats?.outputRows?.indices || 0).toLocaleString()}
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
                {silverStatus.performance?.totalDuration || 0}s
              </div>
            </motion.div>
                        <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center glass"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Throughput</div>
              <div className="text-lg font-display font-bold text-light-green">
                {(silverStatus.performance?.throughput || 0).toLocaleString()} rows/s
              </div>
            </motion.div>
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center glass"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Last Run</div>
              <div className="text-lg font-display font-bold text-crystal-white">
                {silverStatus.lastRun ? new Date(silverStatus.lastRun).toLocaleTimeString() : 'Never'}
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
            {/* Data Cleaning */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('dataCleaning')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-center space-x-3 justify-center w-full">
                  <span className="text-xl">🧹</span>
                  <span className="font-display font-semibold text-pure-white">Data Cleaning</span>
                </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('dataCleaning') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('dataCleaning') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• Remove duplicates and null values</li>
                      <li>• Standardize data formats</li>
                      <li>• Handle missing data</li>
                      <li>• Validate data integrity</li>
                    </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Data Transformation */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('dataTransformation')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-center space-x-3 justify-center w-full">
                  <span className="text-xl">🔄</span>
                  <span className="font-display font-semibold text-pure-white">Data Transformation</span>
                </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('dataTransformation') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('dataTransformation') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• Convert data types</li>
                      <li>• Normalize values</li>
                      <li>• Apply business rules</li>
                      <li>• Create derived fields</li>
                    </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Feature Engineering */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden glass"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('featureEngineering')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-center space-x-3 justify-center w-full">
                  <span className="text-xl">⚙️</span>
                  <span className="font-display font-semibold text-pure-white">Feature Engineering</span>
                </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('featureEngineering') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('featureEngineering') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• Create technical indicators</li>
                      <li>• Calculate moving averages</li>
                      <li>• Generate lag features</li>
                      <li>• Build composite metrics</li>
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
            onClick={startSilverLayer}
            disabled={isRunning}
                            className="px-24 py-3 bg-light-green text-background-primary font-display font-semibold rounded-lg hover:bg-green-400 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-200"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            {isRunning ? 'Running...' : 'Run Silver Layer'}
          </motion.button>
        </motion.div>
      </motion.div>
    </motion.div>
  );
};

export default SilverLayerMonitor;

