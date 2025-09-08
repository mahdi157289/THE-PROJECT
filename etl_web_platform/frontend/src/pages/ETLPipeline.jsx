import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import EnhancedBronzeLayer from '../components/bronze/EnhancedBronzeLayer';
import SilverLayerMonitor from '../components/silver/SilverLayerMonitor';
import GoldenLayerMonitor from '../components/golden/GoldenLayerMonitor';
import DiamondLayerMonitor from '../components/diamond/DiamondLayerMonitor';

// Medal component for ETL layers
const LayerMedal = ({ layer, size = "w-10 h-10" }) => {
  const getMedalConfig = (layer) => {
    switch (layer.toLowerCase()) {
      case 'bronze':
        return {
          medal: '🥉',
          color: 'text-amber-500'
        }
      case 'silver':
        return {
          medal: '🥈',
          color: 'text-gray-400'
        }
      case 'golden':
        return {
          medal: '🥇',
          color: 'text-yellow-400'
        }
      case 'diamond':
        return {
          medal: '💎',
          color: 'text-blue-400'
        }
      default:
        return {
          medal: '🏆',
          color: 'text-gray-400'
        }
    }
  }

  const config = getMedalConfig(layer)

  return (
    <motion.div 
      className={`${size} flex items-center justify-center`}
      whileHover={{ scale: 1.2, rotate: 5 }}
      whileTap={{ scale: 0.9 }}
      transition={{ duration: 0.2 }}
    >
      <span className={`text-4xl ${config.color} drop-shadow-lg`}>
        {config.medal}
      </span>
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

export default function ETLPipeline() {
  const [pipelines, setPipelines] = useState([
    {
      id: 1,
      name: 'Bronze Layer',
      description: 'Raw data ingestion and storage',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0s',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'On-demand',
      config: {
        source: 'BVMT Scraper',
        destination: 'PostgreSQL',
        schedule: 'On-demand',
        retention: '30 days'
      }
    },
    {
      id: 2,
      name: 'Silver Layer',
      description: 'Data cleaning and validation',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0s',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'On-demand',
      config: {
        source: 'Bronze Layer',
        destination: 'PostgreSQL',
        schedule: 'On-demand',
        retention: '30 days'
      }
    },
    {
      id: 3,
      name: 'Golden Layer',
      description: 'Business logic and transformations',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0s',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'On-demand',
      config: {
        source: 'Silver Layer',
        destination: 'PostgreSQL',
        schedule: 'On-demand',
        retention: '30 days'
      }
    },
    {
      id: 4,
      name: 'Diamond Layer',
      description: 'Advanced analytics and ML features',
      status: 'idle',
      progress: 0,
      recordsProcessed: 0,
      totalRecords: 0,
      duration: '0s',
      errors: 0,
      warnings: 0,
      lastRun: 'Never',
      nextRun: 'On-demand',
      config: {
        source: 'Golden Layer',
        destination: 'PostgreSQL',
        schedule: 'On-demand',
        retention: '30 days'
      }
    }
  ]);

  // Fetch real data from all layer APIs
  useEffect(() => {
    const fetchAllLayerData = async () => {
      try {
        // Fetch data from all layer APIs
        const [bronzeRes, silverRes, goldenRes, diamondRes] = await Promise.all([
          fetch('http://127.0.0.1:5000/api/bronze/status'),
          fetch('http://127.0.0.1:5000/api/silver/status'),
          fetch('http://127.0.0.1:5000/api/golden/status'),
          fetch('http://127.0.0.1:5000/api/diamond/status')
        ]);

        const [bronzeData, silverData, goldenData, diamondData] = await Promise.all([
          bronzeRes.json(),
          silverRes.json(),
          goldenRes.json(),
          diamondRes.json()
        ]);

        // Update pipelines with real data
        setPipelines(prev => prev.map(pipeline => {
          let layerData;
          switch (pipeline.name) {
            case 'Bronze Layer':
              layerData = bronzeData.data;
              return {
                ...pipeline,
                status: layerData.status || 'idle',
                progress: layerData.progress || 0,
                recordsProcessed: layerData.dataStats?.cotations?.rows || 0,
                totalRecords: (layerData.dataStats?.cotations?.rows || 0) + (layerData.dataStats?.indices?.rows || 0),
                duration: layerData.performance?.totalDuration ? `${Math.round(layerData.performance.totalDuration)}s` : '0s',
                lastRun: layerData.lastRun ? new Date(layerData.lastRun).toLocaleTimeString() : 'Never',
                errors: 0,
                warnings: 0
              };
            case 'Silver Layer':
              layerData = silverData.data;
              return {
                ...pipeline,
                status: layerData.status || 'idle',
                progress: layerData.progress || 0,
                recordsProcessed: layerData.dataStats?.outputRows?.cotations || 0,
                totalRecords: (layerData.dataStats?.inputRows?.cotations || 0) + (layerData.dataStats?.inputRows?.indices || 0),
                duration: layerData.dataStats?.transformationStats?.processingTime ? `${Math.round(layerData.dataStats.transformationStats.processingTime)}s` : '0s',
                lastRun: layerData.lastRun ? new Date(layerData.lastRun).toLocaleTimeString() : 'Never',
                errors: 0,
                warnings: 0
              };
            case 'Golden Layer':
              layerData = goldenData.data;
              return {
                ...pipeline,
                status: layerData.status || 'idle',
                progress: layerData.progress || 0,
                recordsProcessed: layerData.dataStats?.outputRows?.cotations || 0,
                totalRecords: (layerData.dataStats?.inputRows?.cotations || 0) + (layerData.dataStats?.inputRows?.indices || 0),
                duration: layerData.dataStats?.transformationStats?.processingTime ? `${Math.round(layerData.dataStats.transformationStats.processingTime)}s` : '0s',
                lastRun: layerData.lastRun ? new Date(layerData.lastRun).toLocaleTimeString() : 'Never',
                errors: 0,
                warnings: 0
              };
            case 'Diamond Layer':
              layerData = diamondData.data;
              return {
                ...pipeline,
                status: layerData.status || 'idle',
                progress: layerData.progress || 0,
                recordsProcessed: layerData.dataStats?.outputRows?.cotations || 0,
                totalRecords: (layerData.dataStats?.inputRows?.cotations || 0) + (layerData.dataStats?.inputRows?.indices || 0),
                duration: layerData.dataStats?.transformationStats?.processingTime ? `${Math.round(layerData.dataStats.transformationStats.processingTime)}s` : '0s',
                lastRun: layerData.lastRun ? new Date(layerData.lastRun).toLocaleTimeString() : 'Never',
                errors: 0,
                warnings: 0
              };
            default:
              return pipeline;
          }
        }));
      } catch (error) {
        console.error('Failed to fetch layer data:', error);
      }
    };

    fetchAllLayerData();
    const interval = setInterval(fetchAllLayerData, 5000);
    return () => clearInterval(interval);
  }, []);

  // Handle layer execution
  const handleLayerAction = async (pipelineName, action) => {
    try {
      let endpoint;
      switch (pipelineName) {
        case 'Bronze Layer':
          endpoint = 'http://127.0.0.1:5000/api/bronze/run';
          break;
        case 'Silver Layer':
          endpoint = 'http://127.0.0.1:5000/api/silver/run';
          break;
        case 'Golden Layer':
          endpoint = 'http://127.0.0.1:5000/api/golden/run';
          break;
        case 'Diamond Layer':
          endpoint = 'http://127.0.0.1:5000/api/diamond/run';
          break;
        default:
          console.error('Unknown pipeline:', pipelineName);
          return;
      }

      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });

      const data = await response.json();
      
      if (data.status === 'success') {
        console.log(`${pipelineName} ${action} started successfully`);
        // Update the specific pipeline status immediately
        setPipelines(prev => prev.map(pipeline => 
          pipeline.name === pipelineName 
            ? { ...pipeline, status: 'running', progress: 0 }
            : pipeline
        ));
      } else {
        console.error(`Failed to ${action} ${pipelineName}:`, data.message);
      }
    } catch (error) {
      console.error(`Error ${action}ing ${pipelineName}:`, error);
    }
  };

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

  return (
    <motion.div 
      className="space-y-6"
      variants={containerVariants}
      initial="hidden"
      animate="visible"
    >
      {/* Header */}
      <motion.div variants={itemVariants}>
        <h1 className="text-4xl font-serif font-bold text-pure-white mb-2 text-center">ETL Pipeline Monitor</h1>
        <p className="text-lg text-crystal-white font-sans text-center">
          Monitor and manage your Medallion Architecture data pipeline layers
        </p>
        
        {/* Long Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>
      </motion.div>

      {/* Pipeline Overview */}
      <motion.div 
        className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-6"
        variants={containerVariants}
      >
        {pipelines.map((pipeline, index) => (
          <motion.div
            key={pipeline.id}
            className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 hover-lift glass"
            variants={cardVariants}
            whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" }}
          >
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center">
                <LayerMedal layer={pipeline.name.split(' ')[0]} />
                <div className="ml-3">
                  <h3 className="text-lg font-display font-semibold text-pure-white text-center">{pipeline.name}</h3>
                  <p className="text-sm text-crystal-white text-center">{pipeline.description}</p>
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
              <div className="mb-4">
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

            <div className="space-y-3 text-sm">
              <div className="flex justify-between">
                <span className="text-crystal-white">Records</span>
                <span className="font-display font-medium text-pure-white">
                  {pipeline.recordsProcessed.toLocaleString()} / {pipeline.totalRecords.toLocaleString()}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-crystal-white">Duration</span>
                <span className="font-display font-medium text-pure-white">{pipeline.duration}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-crystal-white">Errors/Warnings</span>
                <span className="font-display font-medium">
                  <span className="text-red-400">{pipeline.errors}</span>
                  <span className="text-crystal-white mx-1">/</span>
                  <span className="text-yellow-400">{pipeline.warnings}</span>
                </span>
              </div>
            </div>

            <div className="mt-4 pt-4 border-t border-light-silver">
              <div className="space-y-2 text-xs">
                <div className="flex justify-between">
                  <span className="text-pearl-white">Last Run</span>
                  <span className="text-crystal-white">{pipeline.lastRun}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-pearl-white">Next Run</span>
                  <span className="text-crystal-white">{pipeline.nextRun}</span>
                </div>
              </div>
            </div>

            <div className="mt-4 flex space-x-2 justify-center">
              <motion.button 
                className="bg-light-green hover:bg-primary-600 text-dark-bg font-display font-medium py-2 px-6 rounded-lg transition-all duration-300 text-sm"
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                onClick={() => handleLayerAction(pipeline.name, pipeline.status === 'running' ? 'stop' : 'start')}
                disabled={pipeline.status === 'running'}
              >
                {pipeline.status === 'running' ? 'Running...' : 'Start'}
              </motion.button>
              <motion.button 
                className="bg-crystal-white hover:bg-secondary-600 text-dark-bg font-display font-medium py-2 px-6 rounded-lg transition-all duration-300 text-sm"
                whileHover={{ scale: 1.05 }}
                whileTap={{ scale: 0.95 }}
                onClick={() => console.log(`Configure ${pipeline.name}`)}
              >
                Configure
              </motion.button>
            </div>
          </motion.div>
        ))}
      </motion.div>

      {/* Interrupted Line */}
      <motion.div 
        className="flex justify-center my-8"
        variants={itemVariants}
      >
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
      </motion.div>

      {/* Layer Details */}
      <motion.div 
        className="space-y-6"
        variants={containerVariants}
      >
        <motion.div variants={itemVariants}>
          <h2 className="text-2xl font-serif font-semibold text-pure-white mb-6 text-center">Layer Details</h2>
        </motion.div>

        <motion.div variants={cardVariants}>
          <EnhancedBronzeLayer />
        </motion.div>

        {/* Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>

        <motion.div variants={cardVariants}>
          <SilverLayerMonitor />
        </motion.div>

        {/* Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>

        <motion.div variants={cardVariants}>
          <GoldenLayerMonitor />
        </motion.div>

        {/* Interrupted Line */}
        <motion.div 
          className="flex justify-center my-6"
          variants={itemVariants}
        >
          <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
        </motion.div>

        <motion.div variants={cardVariants}>
          <DiamondLayerMonitor />
        </motion.div>
      </motion.div>
    </motion.div>
  );
}
