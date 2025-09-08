import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import powerBIConfig from '../../config/powerbi';

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

const PowerBIDashboard = () => {
  const [isLoading, setIsLoading] = useState(true);
  const [dashboardError, setDashboardError] = useState(null);

  useEffect(() => {
    // Simulate loading time for Power BI dashboard
    const timer = setTimeout(() => {
      setIsLoading(false);
    }, 2000);

    return () => clearTimeout(timer);
  }, []);

  const handleIframeLoad = () => {
    setIsLoading(false);
    setDashboardError(null);
  };

  const handleIframeError = () => {
    setIsLoading(false);
    setDashboardError("Failed to load Power BI dashboard. Please check your connection and try again.");
  };

  const refreshDashboard = () => {
    setIsLoading(true);
    setDashboardError(null);
    // Force iframe refresh by changing the key
    window.location.reload();
  };

  return (
    <motion.div 
      className="min-h-screen bg-dark-bg"
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {/* Header Section */}
      <motion.div 
        className="bg-background-secondary shadow-lg border-b border-light-silver"
        variants={itemVariants}
      >
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between py-6">
            <div className="flex items-center space-x-4">
              <motion.div 
                className="w-12 h-12 bg-gradient-to-r from-light-green to-crystal-white rounded-lg flex items-center justify-center"
                whileHover={{ scale: 1.1, rotate: 5 }}
                whileTap={{ scale: 0.9 }}
              >
                <span className="text-2xl">📊</span>
              </motion.div>
              <div>
                <h1 className="text-2xl font-serif font-bold text-pure-white text-center">Power BI Dashboard</h1>
                <p className="text-crystal-white font-sans text-center">Real-time financial analytics and insights</p>
                
                {/* Long Interrupted Line */}
                <motion.div 
                  className="flex justify-center mt-4"
                  variants={itemVariants}
                >
                  <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
                </motion.div>
              </div>
            </div>
            
            <div className="flex items-center space-x-3">
              <motion.button
                onClick={refreshDashboard}
                className="px-4 py-2 bg-light-green hover:bg-primary-600 text-dark-bg rounded-lg font-display font-medium transition-all duration-300 flex items-center space-x-2"
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
              >
                <motion.span
                  animate={{ rotate: isLoading ? 360 : 0 }}
                  transition={{ duration: 1, repeat: isLoading ? Infinity : 0 }}
                >
                  🔄
                </motion.span>
                <span>Refresh Dashboard</span>
              </motion.button>
              
              <motion.div 
                className="px-3 py-1 bg-light-green bg-opacity-20 text-light-green rounded-full text-sm font-display font-medium border border-light-green"
                animate={{ scale: [1, 1.05, 1] }}
                transition={{ duration: 2, repeat: Infinity }}
              >
                Live Data
              </motion.div>
            </div>
          </div>
        </div>
      </motion.div>

      {/* Dashboard Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Dashboard Info Cards */}
        <motion.div 
          className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8"
          variants={containerVariants}
        >
          <motion.div 
            className="bg-background-secondary rounded-lg shadow-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
          >
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-display font-medium text-crystal-white">Dashboard Status</p>
                <p className="text-2xl font-display font-bold text-light-green">Active</p>
              </div>
              <motion.div 
                className="w-8 h-8 bg-light-green bg-opacity-20 rounded-full flex items-center justify-center"
                animate={{ scale: [1, 1.2, 1] }}
                transition={{ duration: 2, repeat: Infinity }}
              >
                <span className="text-light-green">✅</span>
              </motion.div>
            </div>
          </motion.div>

          <motion.div 
            className="bg-background-secondary rounded-lg shadow-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
          >
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-display font-medium text-crystal-white">Data Source</p>
                <p className="text-2xl font-display font-bold text-crystal-white">PostgreSQL</p>
              </div>
              <motion.div 
                className="w-8 h-8 bg-crystal-white bg-opacity-20 rounded-full flex items-center justify-center"
                whileHover={{ scale: 1.1 }}
              >
                <span className="text-crystal-white">🗄️</span>
              </motion.div>
            </div>
          </motion.div>

          <motion.div 
            className="bg-background-secondary rounded-lg shadow-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
          >
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-display font-medium text-crystal-white">Last Updated</p>
                <p className="text-2xl font-display font-bold text-light-green">Live</p>
              </div>
              <motion.div 
                className="w-8 h-8 bg-light-green bg-opacity-20 rounded-full flex items-center justify-center"
                animate={{ pulse: true }}
              >
                <span className="text-light-green">⚡</span>
              </motion.div>
            </div>
          </motion.div>

          <motion.div 
            className="bg-background-secondary rounded-lg shadow-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
          >
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-display font-medium text-crystal-white">Connection</p>
                <p className="text-2xl font-display font-bold text-light-green">Secure</p>
              </div>
              <motion.div 
                className="w-8 h-8 bg-light-green bg-opacity-20 rounded-full flex items-center justify-center"
                whileHover={{ scale: 1.1 }}
              >
                <span className="text-light-green">🔒</span>
              </motion.div>
            </div>
          </motion.div>
        </motion.div>

        {/* Interrupted Line */}
        <motion.div 
          className="flex justify-center my-8"
          variants={itemVariants}
        >
          <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>

        {/* Power BI Dashboard Container */}
        <motion.div 
          className="bg-background-secondary rounded-lg shadow-lg border border-light-silver overflow-hidden"
          variants={cardVariants}
          whileHover={{ scale: 1.01 }}
        >
          <div className="p-6 border-b border-light-silver">
            <h2 className="text-xl font-display font-semibold text-pure-white mb-2">Financial Analytics Dashboard</h2>
            <p className="text-crystal-white font-sans">Interactive Power BI reports with real-time data from BVMT</p>
          </div>

          <div className="relative h-96">
            {isLoading && (
              <motion.div 
                className="absolute inset-0 bg-background-tertiary flex items-center justify-center"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
              >
                <div className="text-center">
                  <motion.div
                    className="w-16 h-16 border-4 border-light-green border-t-transparent rounded-full mx-auto mb-4"
                    animate={{ rotate: 360 }}
                    transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                  />
                  <p className="text-crystal-white font-sans">Loading Power BI Dashboard...</p>
                </div>
              </motion.div>
            )}

            {dashboardError && (
              <motion.div 
                className="absolute inset-0 bg-background-tertiary flex items-center justify-center"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
              >
                <div className="text-center p-6">
                  <div className="text-red-400 text-4xl mb-4">⚠️</div>
                  <p className="text-red-400 font-sans mb-4">{dashboardError}</p>
                  <motion.button
                    onClick={refreshDashboard}
                    className="px-4 py-2 bg-light-green hover:bg-primary-600 text-dark-bg rounded-lg font-display font-medium transition-all duration-300"
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    Try Again
                  </motion.button>
                </div>
              </motion.div>
            )}

            {!isLoading && !dashboardError && (
              <motion.iframe
                src={powerBIConfig.embedUrl}
                className="w-full h-full"
                onLoad={handleIframeLoad}
                onError={handleIframeError}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ duration: 0.5 }}
                title="Power BI Dashboard"
                frameBorder="0"
                allowFullScreen
              />
            )}
          </div>
        </motion.div>

        {/* Interrupted Line */}
        <motion.div 
          className="flex justify-center my-8"
          variants={itemVariants}
        >
          <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>

        {/* Dashboard Features */}
        <motion.div 
          className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-6"
          variants={containerVariants}
        >
          <motion.div 
            className="bg-background-secondary rounded-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02 }}
          >
            <h3 className="text-lg font-display font-semibold text-pure-white mb-3">📈 Market Analysis</h3>
            <p className="text-crystal-white font-sans text-sm">
              Real-time stock price analysis, market trends, and performance indicators for Tunisian companies.
            </p>
          </motion.div>

          <motion.div 
            className="bg-background-secondary rounded-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02 }}
          >
            <h3 className="text-lg font-display font-semibold text-pure-white mb-3">💰 Financial Metrics</h3>
            <p className="text-crystal-white font-sans text-sm">
              Key financial ratios, profitability analysis, and risk assessment metrics for informed decision-making.
            </p>
          </motion.div>

          <motion.div 
            className="bg-background-secondary rounded-lg p-6 border border-light-silver glass"
            variants={cardVariants}
            whileHover={{ scale: 1.02 }}
          >
            <h3 className="text-lg font-display font-semibold text-pure-white mb-3">🔍 Predictive Insights</h3>
            <p className="text-crystal-white font-sans text-sm">
              Machine learning-powered predictions and forecasting models for market behavior analysis.
            </p>
          </motion.div>
        </motion.div>
      </div>
    </motion.div>
  );
};

export default PowerBIDashboard;
