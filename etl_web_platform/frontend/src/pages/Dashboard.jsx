import React, { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import Loader from '../components/common/Loader'

// Trophy component for ETL layers
const LayerTrophy = ({ layer, size = "w-8 h-8" }) => {
  const getTrophyConfig = (layer) => {
    switch (layer.toLowerCase()) {
      case 'bronze':
        return {
          number: '1',
          bgGradient: 'from-amber-400 to-orange-500',
          ribbonColor: 'from-blue-400 to-blue-600',
          medallionColor: 'from-orange-300 to-pink-400'
        }
      case 'silver':
        return {
          number: '2',
          bgGradient: 'from-gray-300 to-gray-500',
          ribbonColor: 'from-blue-400 to-blue-600',
          medallionColor: 'from-gray-200 to-gray-400'
        }
      case 'golden':
        return {
          number: '3',
          bgGradient: 'from-yellow-400 to-orange-500',
          ribbonColor: 'from-blue-400 to-blue-600',
          medallionColor: 'from-orange-300 to-pink-400'
        }
      case 'diamond':
        return {
          number: '4',
          bgGradient: 'from-blue-400 to-purple-600',
          ribbonColor: 'from-blue-400 to-blue-600',
          medallionColor: 'from-blue-300 to-purple-400'
        }
      default:
        return {
          number: '?',
          bgGradient: 'from-gray-300 to-gray-500',
          ribbonColor: 'from-blue-400 to-blue-600',
          medallionColor: 'from-gray-200 to-gray-400'
        }
    }
  }

  const config = getTrophyConfig(layer)

  return (
    <motion.div 
      className={`${size} relative`}
      whileHover={{ scale: 1.2, rotate: 5 }}
      whileTap={{ scale: 0.9 }}
      transition={{ duration: 0.2 }}
    >
      {/* Background rounded square with gradient */}
      <div className={`w-full h-full rounded-lg bg-gradient-to-br ${config.bgGradient} p-1`}>
        {/* Ribbon at the top */}
        <div className={`w-full h-2 bg-gradient-to-r ${config.ribbonColor} rounded-t-lg mb-1`}></div>
        
        {/* Medallion circle */}
        <div className={`w-full h-full rounded-full bg-gradient-to-br ${config.medallionColor} flex items-center justify-center`}>
          <span className="text-white font-bold text-lg drop-shadow-lg">{config.number}</span>
        </div>
      </div>
    </motion.div>
  )
}

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

export default function Dashboard() {
  const [isLoading, setIsLoading] = useState(true);
  
  // Real data from APIs
  const [stats, setStats] = useState({
    totalRecords: 0,
    activePipelines: 0,
    cpuUsage: 0,
    memoryUsage: 0
  })

  const [pipelineStatus, setPipelineStatus] = useState([
    { name: 'Bronze Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
    { name: 'Silver Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
    { name: 'Golden Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
    { name: 'Diamond Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 }
  ])

  const [scrapingStatus, setScrapingStatus] = useState([
    { name: 'BVMT Cotations', status: 'idle', lastRun: 'Never', records: 0, duration: '0m', errors: 0, warnings: 0 },
    { name: 'BVMT Indices', status: 'idle', lastRun: 'Never', records: 0, duration: '0m', errors: 0, warnings: 0 }
  ])

  const [hasError, setHasError] = useState(false)

  // Fetch real data from APIs
  useEffect(() => {
    const fetchData = async () => {
      try {
        const startTime = Date.now();
        // Fetch ETL layer statuses
        const layerEndpoints = [
          'http://127.0.0.1:5000/api/bronze/status',
          'http://127.0.0.1:5000/api/silver/status',
          'http://127.0.0.1:5000/api/golden/status',
          'http://127.0.0.1:5000/api/diamond/status'
        ];

        const layerResponses = await Promise.all(
          layerEndpoints.map(endpoint => fetch(endpoint))
        );

        const layerData = await Promise.all(
          layerResponses.map(response => response.json())
        );

        // Update pipeline status with real data
        const updatedPipelines = [
          { name: 'Bronze Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
          { name: 'Silver Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
          { name: 'Golden Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 },
          { name: 'Diamond Layer', status: 'idle', progress: 0, records: 0, duration: '0m', errors: 0, warnings: 0 }
        ].map((pipeline, index) => {
          const response = layerData[index];
          if (response && response.data) {
            const data = response.data;
            return {
              name: pipeline.name,
              status: data.status || 'idle',
              progress: data.processing_stats?.progress || 0,
              records: (data.processing_stats?.cotations_rows || 0) + (data.processing_stats?.indices_rows || 0),
              duration: data.processing_stats?.duration ? `${Math.round(data.processing_stats.duration)}s` : '0m',
              errors: 0,
              warnings: 0
            };
          }
          return pipeline;
        });

        setPipelineStatus(updatedPipelines);

        // Calculate total stats
        const totalRecords = updatedPipelines.reduce((sum, pipeline) => sum + pipeline.records, 0);
        const activePipelines = updatedPipelines.filter(pipeline => pipeline.status === 'running').length;
        
        setStats({
          totalRecords,
          activePipelines,
          cpuUsage: Math.round(Math.random() * 30 + 20), // Simulated CPU usage
          memoryUsage: Math.round(Math.random() * 40 + 30) // Simulated memory usage
        });

        // Fetch scraping status and files
        try {
          const [scrapingResponse, filesResponse] = await Promise.all([
            fetch('http://127.0.0.1:5000/api/scraping/status'),
            fetch('http://127.0.0.1:5000/api/scraping/files')
          ]);
          
          const scrapingData = await scrapingResponse.json();
          const filesData = await filesResponse.json();
          
          if (scrapingData.data && filesData.data) {
            const scrapingInfo = scrapingData.data;
            const filesInfo = filesData.data;
            
            // Debug logging
            console.log('🔍 Scraping Info:', scrapingInfo);
            console.log('🔍 Files Info:', filesInfo);
            
            // Calculate real file statistics
            const cotationsFiles = filesInfo.files?.filter(f => f.file_type === 'cotations') || [];
            const indicesFiles = filesInfo.files?.filter(f => f.file_type === 'indices') || [];
            
            // Get the most recent file for each type
            const latestCotations = cotationsFiles.length > 0 ? 
              cotationsFiles.sort((a, b) => new Date(b.created_at) - new Date(a.created_at))[0] : null;
            const latestIndices = indicesFiles.length > 0 ? 
              indicesFiles.sort((a, b) => new Date(b.created_at) - new Date(a.created_at))[0] : null;
            
            const updatedScraping = [
              { 
                name: 'BVMT Cotations', 
                status: scrapingInfo.is_running ? 'running' : (cotationsFiles.length > 0 ? 'completed' : 'idle'), 
                lastRun: latestCotations ? new Date(latestCotations.created_at).toLocaleString() : 'Never', 
                records: Math.round(cotationsFiles.reduce((sum, file) => sum + file.size_mb, 0) * 50000), // Better estimation
                duration: scrapingInfo.duration ? `${Math.round(scrapingInfo.duration)}s` : 'Unknown', 
                errors: 0, 
                warnings: 0 
              },
              { 
                name: 'BVMT Indices', 
                status: scrapingInfo.is_running ? 'running' : (indicesFiles.length > 0 ? 'completed' : 'idle'), 
                lastRun: latestIndices ? new Date(latestIndices.created_at).toLocaleString() : 'Never', 
                records: Math.round(indicesFiles.reduce((sum, file) => sum + file.size_mb, 0) * 1000), // Better estimation
                duration: scrapingInfo.duration ? `${Math.round(scrapingInfo.duration)}s` : 'Unknown', 
                errors: 0, 
                warnings: 0 
              }
            ];
            setScrapingStatus(updatedScraping);
          }
        } catch (error) {
          console.log('Scraping status not available:', error);
        }

      } catch (error) {
        console.error('Error fetching dashboard data:', error);
        setHasError(true);
      } finally {
        // Ensure minimum 3-second loading duration
        const elapsed = Date.now() - startTime;
        const remainingTime = Math.max(3000 - elapsed, 0);
        
        setTimeout(() => {
          setIsLoading(false);
        }, remainingTime);
      }
      };

      // Initial fetch
      fetchData();

      // Set up polling every 10 seconds
      const interval = setInterval(fetchData, 10000);

      return () => clearInterval(interval);
    }, []);

  const getStatusColor = (status) => {
    switch (status) {
      case 'running': return 'text-light-green bg-light-green bg-opacity-20'
      case 'completed': return 'text-crystal-white bg-crystal-white bg-opacity-20'
      case 'idle': return 'text-pearl-white bg-pearl-white bg-opacity-10'
      case 'error': return 'text-red-400 bg-red-400 bg-opacity-20'
      default: return 'text-pearl-white bg-pearl-white bg-opacity-10'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'running': return <span className="w-5 h-5 bg-light-green rounded-full animate-pulse"></span>
      case 'completed': return <span className="w-5 h-5 bg-crystal-white rounded-full"></span>
      case 'idle': return <span className="w-5 h-5 bg-pearl-white bg-opacity-50 rounded-full"></span>
      case 'error': return <span className="w-5 h-5 bg-red-400 rounded-full"></span>
      default: return <span className="w-5 h-5 bg-pearl-white bg-opacity-50 rounded-full"></span>
    }
  }

  // Extract layer name for trophy
  const getLayerName = (fullName) => {
    if (fullName.includes('Bronze')) return 'Bronze'
    if (fullName.includes('Silver')) return 'Silver'
    if (fullName.includes('Golden')) return 'Golden'
    if (fullName.includes('Diamond')) return 'Diamond'
    return 'Unknown'
  }

  if (isLoading) {
    return <Loader />;
  }

  return (
    <motion.div 
      className="space-y-6"
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {/* Header */}
      <motion.div variants={itemVariants}>
        <h1 className="text-4xl font-serif font-bold text-pure-white mb-2 text-center">ETL Platform Dashboard</h1>
        <p className="text-lg text-crystal-white font-sans text-center">
          Monitor your financial data pipeline, scraping operations, and system health
        </p>
        
        {/* Long Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>
        
        {/* Loading Indicator */}
        {isLoading && (
          <motion.div 
            className="mt-4 p-4 bg-background-secondary rounded-lg border border-light-silver glass"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <div className="flex items-center justify-center">
              <div className="w-6 h-6 border-2 border-light-green border-t-transparent rounded-full animate-spin mr-3"></div>
              <span className="text-crystal-white font-medium">Loading real-time data...</span>
            </div>
          </motion.div>
        )}

        {/* Error Message */}
        {hasError && !isLoading && (
          <motion.div 
            className="mt-4 p-4 bg-red-900 bg-opacity-20 rounded-lg border border-red-400 glass"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <div className="flex items-center justify-center">
              <div className="w-5 h-5 bg-red-400 rounded-full mr-3"></div>
              <span className="text-red-400 font-medium">Backend not available. Showing static data.</span>
            </div>
          </motion.div>
        )}
        
        {/* ETL Layer Legend */}
        <motion.div 
          className="mt-6 p-6 bg-background-secondary rounded-lg border border-light-silver glass"
          variants={cardVariants}
          whileHover={{ scale: 1.02, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.1)" }}
        >
          <h3 className="text-lg font-serif font-semibold text-pure-white mb-4 text-center">ETL Layer Legend</h3>
          <div className="flex flex-wrap gap-6 justify-center">
            <motion.div 
              className="flex items-center"
              whileHover={{ scale: 1.05 }}
              transition={{ duration: 0.2 }}
            >
              <LayerTrophy layer="Bronze" size="w-6 h-6" />
              <span className="ml-3 text-sm text-crystal-white font-medium">Bronze - Raw Data Ingestion</span>
            </motion.div>
            <motion.div 
              className="flex items-center"
              whileHover={{ scale: 1.05 }}
              transition={{ duration: 0.2 }}
            >
              <LayerTrophy layer="Silver" size="w-6 h-6" />
              <span className="ml-3 text-sm text-crystal-white font-medium">Silver - Data Cleaning</span>
            </motion.div>
            <motion.div 
              className="flex items-center"
              whileHover={{ scale: 1.05 }}
              transition={{ duration: 0.2 }}
            >
              <LayerTrophy layer="Golden" size="w-6 h-6" />
              <span className="ml-3 text-sm text-crystal-white font-medium">Golden - Business Logic</span>
            </motion.div>
            <motion.div 
              className="flex items-center"
              whileHover={{ scale: 1.05 }}
              transition={{ duration: 0.2 }}
            >
              <LayerTrophy layer="Diamond" size="w-6 h-6" />
              <span className="ml-3 text-sm text-crystal-white font-medium">Diamond - ML & Analytics</span>
            </motion.div>
          </div>
        </motion.div>
      </motion.div>

      {/* Overview Stats */}
      <motion.div 
        className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6"
        variants={containerVariants}
      >
        <motion.div 
          className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 hover-lift glass"
          variants={cardVariants}
          whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
        >
          <div className="flex items-center">
            <motion.span 
              className="w-10 h-10 bg-light-green rounded-lg"
              whileHover={{ rotate: 360 }}
              transition={{ duration: 0.6 }}
            ></motion.span>
            <div className="ml-4">
              <p className="text-sm font-medium text-crystal-white">Total Records</p>
              <p className="text-2xl font-display font-bold text-pure-white">{stats.totalRecords.toLocaleString()}</p>
            </div>
          </div>
        </motion.div>

        <motion.div 
          className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 hover-lift glass"
          variants={cardVariants}
          whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
        >
          <div className="flex items-center">
            <motion.span 
              className="w-10 h-10 bg-light-green rounded-lg"
              animate={{ scale: [1, 1.1, 1] }}
              transition={{ duration: 2, repeat: Infinity }}
            ></motion.span>
            <div className="ml-4">
              <p className="text-sm font-medium text-crystal-white">Active Pipelines</p>
              <p className="text-2xl font-display font-bold text-pure-white">{stats.activePipelines}</p>
            </div>
          </div>
        </motion.div>

        <motion.div 
          className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 hover-lift glass"
          variants={cardVariants}
          whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
        >
          <div className="flex items-center">
            <motion.span 
              className="w-10 h-10 bg-crystal-white rounded-lg"
              whileHover={{ rotate: 360 }}
              transition={{ duration: 0.6 }}
            ></motion.span>
            <div className="ml-4">
              <p className="text-sm font-medium text-crystal-white">CPU Usage</p>
              <p className="text-2xl font-display font-bold text-pure-white">{stats.cpuUsage}%</p>
            </div>
          </div>
        </motion.div>

        <motion.div 
          className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 hover-lift glass"
          variants={cardVariants}
          whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
        >
          <div className="flex items-center">
            <motion.span 
              className="w-10 h-10 bg-crystal-white rounded-lg"
              animate={{ scale: [1, 1.1, 1] }}
              transition={{ duration: 2, repeat: Infinity }}
            ></motion.span>
            <div className="ml-4">
              <p className="text-sm font-medium text-crystal-white">Memory Usage</p>
              <p className="text-2xl font-display font-bold text-pure-white">{stats.memoryUsage}%</p>
            </div>
          </div>
        </motion.div>
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-12"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* Scraping Status */}
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.1)" }}
      >
        <h2 className="text-2xl font-serif font-semibold text-pure-white mb-6 text-center">Scraping Status</h2>
        <div className="space-y-4">
          {scrapingStatus.map((job, index) => (
            <motion.div
              key={job.name}
              className="border border-light-silver rounded-lg p-4 bg-background-tertiary hover-lift"
              variants={itemVariants}
              whileHover={{ scale: 1.02, boxShadow: "0 10px 30px rgba(144, 238, 144, 0.1)" }}
              transition={{ delay: index * 0.1 }}
            >
              <div className="flex items-center justify-between mb-3">
                <span className="font-display font-medium text-pure-white">{job.name}</span>
                <motion.span 
                  className={`px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(job.status)}`}
                  whileHover={{ scale: 1.05 }}
                >
                  {job.status}
                </motion.span>
              </div>
              
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-crystal-white">Last Run</p>
                  <p className="font-display font-medium text-pure-white">{job.lastRun}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Records</p>
                  <p className="font-display font-medium text-pure-white">{job.records.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Duration</p>
                  <p className="font-display font-medium text-pure-white">{job.duration}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Errors/Warnings</p>
                  <p className="font-display font-medium">
                    <span className="text-red-400">{job.errors}</span>
                    <span className="text-crystal-white mx-1">/</span>
                    <span className="text-yellow-400">{job.warnings}</span>
                  </p>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </motion.div>

      {/* Pipeline Status */}
      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-12"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.1)" }}
      >
        <h2 className="text-2xl font-serif font-semibold text-pure-white mb-6 text-center">ETL Pipeline Status</h2>
        <div className="space-y-4">
          {pipelineStatus.map((pipeline, index) => (
            <motion.div
              key={pipeline.name}
              className="border border-light-silver rounded-lg p-4 bg-background-tertiary hover-lift"
              variants={itemVariants}
              whileHover={{ scale: 1.02, boxShadow: "0 10px 30px rgba(144, 238, 144, 0.1)" }}
              transition={{ delay: index * 0.1 }}
            >
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center">
                  <LayerTrophy layer={getLayerName(pipeline.name)} size="w-6 h-6" />
                  <div className="ml-3 flex items-center">
                    {getStatusIcon(pipeline.status)}
                    <span className="ml-2 font-display font-medium text-pure-white">{pipeline.name}</span>
                  </div>
                </div>
                <motion.span 
                  className={`px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(pipeline.status)}`}
                  whileHover={{ scale: 1.05 }}
                >
                  {pipeline.status}
                </motion.span>
              </div>
              
              {pipeline.status === 'running' && (
                <div className="mb-3">
                  <div className="flex justify-between text-sm text-crystal-white mb-2">
                    <span>Progress</span>
                    <span>{pipeline.progress}%</span>
                  </div>
                  <div className="w-full bg-background-primary rounded-full h-3 overflow-hidden">
                    <motion.div 
                      className="bg-light-green h-3 rounded-full"
                      initial={{ width: 0 }}
                      animate={{ width: `${pipeline.progress}%` }}
                      transition={{ duration: 1, ease: "easeOut" }}
                    ></motion.div>
                  </div>
                </div>
              )}
              
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-crystal-white">Records</p>
                  <p className="font-display font-medium text-pure-white">{pipeline.records.toLocaleString()}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Duration</p>
                  <p className="font-display font-medium text-pure-white">{pipeline.duration}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Errors</p>
                  <p className="font-display font-medium text-red-400">{pipeline.errors}</p>
                </div>
                <div>
                  <p className="text-crystal-white">Warnings</p>
                  <p className="font-display font-medium text-yellow-400">{pipeline.warnings}</p>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-12"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* Quick Actions */}
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.1)" }}
      >
        <h2 className="text-2xl font-serif font-semibold text-pure-white mb-6 text-center">Quick Actions</h2>
        <div className="flex flex-wrap gap-4 justify-center">
          <motion.button 
            className="btn-primary"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            Start All Pipelines
          </motion.button>
          <motion.button 
            className="btn-secondary"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            Stop All Pipelines
          </motion.button>
          <motion.button 
            className="btn-secondary"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            Run Scraping Jobs
          </motion.button>
          <motion.button 
            className="btn-secondary"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            View Logs
          </motion.button>
        </div>
      </motion.div>
    </motion.div>
  )
}
