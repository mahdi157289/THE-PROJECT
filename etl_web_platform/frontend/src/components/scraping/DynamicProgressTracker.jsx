import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';

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

export default function DynamicProgressTracker({ files, isRunning, scrapingStatus }) {
  const [progress, setProgress] = useState(0);
  const [stats, setStats] = useState({
    totalFiles: 0,
    indicesCount: 0,
    cotationsCount: 0,
    totalSize: 0,
    yearsCovered: 0
  });

  // Calculate progress and stats based on live backend data
  useEffect(() => {
    // 🎯 USE LIVE BACKEND DATA if available
    if (scrapingStatus) {
      console.log('📊 [FRONTEND] Using live scraping status:', scrapingStatus);
      
      // Use backend's calculated progress
      const currentProgress = scrapingStatus.progress || 0;
      
      setProgress(currentProgress);
      setStats({
        totalFiles: scrapingStatus.total_files_created || 0,
        indicesCount: scrapingStatus.indices_count || 0,
        cotationsCount: scrapingStatus.cotations_count || 0,
        totalSize: 0, // File size not tracked in live status
        yearsCovered: scrapingStatus.successful_years ? scrapingStatus.successful_years.length : 0
      });
      
      console.log(`📊 [FRONTEND] Updated progress: ${currentProgress.toFixed(1)}% | Files: ${scrapingStatus.total_files_created}/34`);
      return;
    }
    
    // 📁 FALLBACK: Use files data if no live status
    if (files && files.length > 0) {
      console.log('📁 [FRONTEND] Fallback to files data');
      
      // Calculate progress based on expected total (17 indices + 17 cotations = 34 files)
      const expectedTotal = 34;
      const currentProgress = Math.min(100, (files.length / expectedTotal) * 100);
      
      // Calculate statistics
      const indicesFiles = files.filter(f => f.file_type === 'indices');
      const cotationsFiles = files.filter(f => f.file_type === 'cotations');
      
      const totalSize = files.reduce((sum, file) => sum + file.size_mb, 0);
      const yearsCovered = new Set(files.map(f => f.year)).size;
      
      setProgress(currentProgress);
      setStats({
        totalFiles: files.length,
        indicesCount: indicesFiles.length,
        cotationsCount: cotationsFiles.length,
        totalSize: totalSize,
        yearsCovered: yearsCovered
      });
    } else {
      console.log('📁 [FRONTEND] No data available, resetting to zero');
      setProgress(0);
      setStats({
        totalFiles: 0,
        indicesCount: 0,
        cotationsCount: 0,
        totalSize: 0,
        yearsCovered: 0
      });
    }
  }, [files, scrapingStatus]);

  const formatFileSize = (sizeMB) => {
    if (sizeMB < 1) {
      return `${(sizeMB * 1024).toFixed(1)} KB`;
    }
    return `${sizeMB.toFixed(2)} MB`;
  };

  const getProgressColor = (progress) => {
    if (progress < 30) return 'bg-red-400';
    if (progress < 70) return 'bg-yellow-400';
    if (progress < 100) return 'bg-blue-400';
    return 'bg-light-green';
  };

  const getProgressText = (progress) => {
    if (progress === 0) return 'Starting...';
    if (progress < 30) return 'Initializing...';
    if (progress < 70) return 'Processing...';
    if (progress < 100) return 'Finalizing...';
    return 'Complete!';
  };

  return (
    <motion.div 
      className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
      variants={cardVariants}
      whileHover={{ scale: 1.01 }}
    >
      {/* Header */}
      <motion.div 
        className="flex items-center justify-between mb-6"
        variants={itemVariants}
      >
        <div>
          <h3 className="text-xl font-display font-semibold text-pure-white">📊 Real-Time Scraping Progress</h3>
          <p className="text-crystal-white font-sans">Monitor BVMT data collection progress</p>
        </div>
        <motion.span 
          className={`px-3 py-1 rounded-full text-xs font-display font-medium ${
            isRunning 
              ? 'text-light-green bg-light-green bg-opacity-20' 
              : 'text-pearl-white bg-pearl-white bg-opacity-10'
          }`}
          whileHover={{ scale: 1.05 }}
        >
          {isRunning ? '🔄 Running' : '⏸️ Stopped'}
        </motion.span>
      </motion.div>

      {/* Progress Bar */}
      <motion.div 
        className="mb-6"
        variants={itemVariants}
      >
        <div className="flex items-center justify-between mb-3">
          <span className="text-sm font-display font-medium text-crystal-white">
            {getProgressText(progress)}
          </span>
          <span className="text-sm font-display font-medium text-crystal-white">
            {progress.toFixed(1)}%
          </span>
        </div>
        <div className="w-full bg-background-primary rounded-full h-3 overflow-hidden">
          <motion.div 
            className={`h-3 rounded-full transition-all duration-500 ease-out ${getProgressColor(progress)}`}
            style={{ width: `${progress}%` }}
            initial={{ width: 0 }}
            animate={{ width: `${progress}%` }}
            transition={{ duration: 1, ease: "easeOut" }}
          ></motion.div>
        </div>
      </motion.div>

      {/* Statistics Grid */}
      <motion.div 
        className="grid grid-cols-2 md:grid-cols-5 gap-4"
        variants={containerVariants}
      >
        <motion.div 
          className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="text-2xl font-display font-bold text-pure-white mb-1">
            {stats.totalFiles}
          </div>
          <div className="text-xs text-crystal-white font-sans">Total Files</div>
        </motion.div>

        <motion.div 
          className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="text-2xl font-display font-bold text-blue-400 mb-1">
            {stats.indicesCount}
          </div>
          <div className="text-xs text-crystal-white font-sans">Indices Files</div>
        </motion.div>

        <motion.div 
          className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="text-2xl font-display font-bold text-green-400 mb-1">
            {stats.cotationsCount}
          </div>
          <div className="text-xs text-crystal-white font-sans">Cotations Files</div>
        </motion.div>

        <motion.div 
          className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="text-2xl font-display font-bold text-light-green mb-1">
            {formatFileSize(stats.totalSize)}
          </div>
          <div className="text-xs text-crystal-white font-sans">Total Size</div>
        </motion.div>

        <motion.div 
          className="bg-background-tertiary rounded-lg p-4 border border-light-silver text-center"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <div className="text-2xl font-display font-bold text-yellow-400 mb-1">
            {stats.yearsCovered}
          </div>
          <div className="text-xs text-crystal-white font-sans">Years Covered</div>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}
