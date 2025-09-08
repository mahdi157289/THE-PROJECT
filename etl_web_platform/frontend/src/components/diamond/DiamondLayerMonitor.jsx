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

export default function DiamondLayerMonitor() {
  const [diamondStatus, setDiamondStatus] = useState({
    status: 'idle',
    processing: false,
    progress: 0,
    current_step: 'Ready to start',
    logs: [],
    real_time_output: [],
    dataStats: {
      inputRows: { cotations: 0, indices: 0 },
      outputRows: { cotations: 0, indices: 0 },
      qualityScore: 0,
      transformationStats: {
        loadedRows: 0,
        processedRows: 0,
        validatedRows: 0,
        processingTime: 0
      },
      statisticalValidation: {
        totalTests: 0,
        passedTests: 0,
        failedTests: 0,
        successRate: 0
      },
      econometricAnalysis: {
        garchModels: 0,
        varModels: 0,
        causalityTests: 0,
        correlationStrategies: 0
      },
      machineLearning: {
        modelsTrained: 0,
        predictionAccuracy: 0,
        trainingTime: 0,
        modelTypes: []
      },
      riskAnalysis: {
        varCalculations: 0,
        expectedShortfall: 0,
        riskAdjustedReturns: 0,
        maxDrawdown: 0
      },
      marketAnalysis: {
        regimeDetection: false,
        sectorAnalysis: false,
        technicalValidation: false,
        tradingSignals: 0
      },
      advancedFeatures: {
        studentTDistribution: false,
        rollingCorrelations: false,
        sectorVolatility: false,
        marketMicrostructure: false,
        anomalyDetection: false,
        stressTesting: false
      },
      performanceMetrics: {
        throughput: 0,
        memoryEfficiency: 0,
        cpuUtilization: 0,
        ioOptimization: 0
      },
      dataQuality: {
        completeness: 0,
        accuracy: 0,
        consistency: 0,
        timeliness: 0
      }
    }
  });
  const [showDetails, setShowDetails] = useState(false);
  const [expandedFeatures, setExpandedFeatures] = useState(new Set());

  // Fetch diamond layer status
  useEffect(() => {
    const fetchStatus = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/diamond/status');
      const data = await response.json();
      
      if (data.status === 'success') {
          setDiamondStatus(data.data);
      }
    } catch (error) {
      console.error('Failed to fetch diamond status:', error);
    }
  };

    fetchStatus();
    const interval = setInterval(fetchStatus, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleRunDiamond = async () => {
    setDiamondStatus(prev => ({ ...prev, processing: true }));
    
    try {
      const response = await fetch('http://127.0.0.1:5000/api/diamond/run', {
        method: 'POST'
      });
      const data = await response.json();
      
      if (data.status === 'success') {
        console.log('Diamond layer started successfully');
      }
    } catch (error) {
      console.error('Failed to start diamond layer:', error);
      setDiamondStatus(prev => ({ ...prev, processing: false }));
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
          <span className="text-4xl mr-3">💎</span>
          <h2 className="text-3xl font-roboto font-bold text-pure-white">Diamond Layer</h2>
        </div>
        <p className="text-lg text-crystal-white font-sans text-center">
          Advanced Analytics & Modeling
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
            <p className="text-crystal-white font-sans text-center">Statistical validation, econometric analysis, deep learning, and risk modeling</p>
          </div>
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
              <span className="text-sm font-display font-medium text-crystal-white">Analytics</span>
              <span className="text-2xl text-pearl-white ml-2">📊</span>
            </div>
            <div className="text-2xl font-display font-bold text-pure-white">
              {(diamondStatus.dataStats?.inputRows?.cotations || 0).toLocaleString()}
            </div>
            <div className="text-xs text-crystal-white font-sans">
              Input Rows • Quality: {diamondStatus.dataStats?.qualityScore || 0}%
            </div>
          </motion.div>

          <motion.div 
            className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
            variants={itemVariants}
            whileHover={{ scale: 1.02 }}
          >
            <div className="flex items-center justify-center mb-2">
              <span className="text-sm font-display font-medium text-crystal-white">Models</span>
              <span className="text-2xl text-pearl-white ml-2">🤖</span>
            </div>
            <div className="text-2xl font-display font-bold text-pure-white">
              {(diamondStatus.dataStats?.outputRows?.cotations || 0).toLocaleString()}
            </div>
            <div className="text-xs text-crystal-white font-sans">
              Output Rows • Processed: {diamondStatus.dataStats?.transformationStats?.processedRows || 0}
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
                {diamondStatus.dataStats?.transformationStats?.processingTime || 0}s
              </div>
            </motion.div>
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Throughput</div>
              <div className="text-lg font-display font-bold text-light-green">
                {(diamondStatus.dataStats?.performanceMetrics?.throughput || 0).toLocaleString()} rows/s
              </div>
            </motion.div>
            <motion.div 
              className="bg-background-tertiary rounded-lg p-3 border border-light-silver text-center"
              whileHover={{ scale: 1.02 }}
            >
              <div className="text-sm font-display font-medium text-crystal-white mb-1">Last Run</div>
              <div className="text-lg font-display font-bold text-crystal-white">
                {diamondStatus.status}
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
            {/* Statistical Analysis */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('statisticalAnalysis')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-center space-x-3 justify-center w-full">
                  <span className="text-xl">📈</span>
                  <span className="font-roboto font-semibold text-pure-white">Statistical Analysis</span>
                </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('statisticalAnalysis') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('statisticalAnalysis') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                <li>• Anderson-Darling normality testing</li>
                <li>• ADF stationarity testing</li>
                <li>• Levene's variance comparison</li>
                <li>• Cointegration analysis</li>
              </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Machine Learning */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('machineLearning')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                              <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">🤖</span>
                <span className="font-roboto font-semibold text-pure-white">Machine Learning</span>
            </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('machineLearning') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('machineLearning') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                <li>• CNN-LSTM architecture</li>
                <li>• Transformer models</li>
                <li>• MLP-Regressor models</li>
                <li>• Sequence-based forecasting</li>
              </ul>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>

            {/* Risk Analysis */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver overflow-hidden"
              whileHover={{ scale: 1.01 }}
            >
              <motion.button
                onClick={() => toggleFeatureExpansion('riskAnalysis')}
                className="w-full p-4 text-left flex items-center justify-between"
                whileHover={{ backgroundColor: 'rgba(144, 238, 144, 0.1)' }}
                whileTap={{ scale: 0.98 }}
              >
                              <div className="flex items-center space-x-3 justify-center w-full">
                <span className="text-xl">⚠️</span>
                <span className="font-roboto font-semibold text-pure-white">Risk Analysis</span>
              </div>
                <motion.span
                  animate={{ rotate: expandedFeatures.has('riskAnalysis') ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                  className="text-crystal-white"
                >
                  ▼
                </motion.span>
              </motion.button>
              <AnimatePresence>
                {expandedFeatures.has('riskAnalysis') && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="px-4 pb-4"
                  >
                    <ul className="text-sm text-crystal-white space-y-1 font-sans text-center">
                      <li>• VaR calculations</li>
                      <li>• Expected Shortfall</li>
                      <li>• Risk-adjusted returns</li>
                      <li>• Maximum drawdown analysis</li>
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
            onClick={handleRunDiamond}
            disabled={diamondStatus.processing}
                            className="px-24 py-3 bg-light-green hover:bg-primary-600 disabled:bg-pearl-white disabled:opacity-50 text-dark-bg font-display font-medium rounded-lg transition-all duration-300 disabled:cursor-not-allowed"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            {diamondStatus.processing ? (
              <motion.div
                className="flex items-center"
                animate={{ rotate: 360 }}
                transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
              >
                <span className="mr-2">⚙️</span>
                Processing...
              </motion.div>
            ) : (
              'Run Diamond Layer'
            )}
          </motion.button>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}


