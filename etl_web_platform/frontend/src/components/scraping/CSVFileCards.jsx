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

export default function CSVFileCards() {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [previewFile, setPreviewFile] = useState(null);
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);

  // Fetch files from backend and set up auto-refresh
  useEffect(() => {
    fetchFiles();
    
    // Auto-refresh every 10 seconds to show new files
    const interval = setInterval(fetchFiles, 10000);
    
    return () => clearInterval(interval);
  }, []);

  const fetchFiles = async () => {
    try {
      if (files.length === 0) {
        setLoading(true);
      }
      const response = await fetch('http://127.0.0.1:5000/api/scraping/files');
      const data = await response.json();
      
      if (data.status === 'success') {
        setFiles(data.data.files);
        setError(null);
        setLastUpdated(new Date());
        console.log(`📁 [CSVFileCards] Loaded ${data.data.files.length} files (${data.data.indices_count} indices, ${data.data.cotations_count} cotations)`);
      } else {
        setError(data.message || 'Failed to fetch files');
        console.error('❌ [CSVFileCards] API error:', data.message);
      }
    } catch (err) {
      setError('Failed to connect to backend');
      console.error('❌ [CSVFileCards] Connection error:', err);
    } finally {
      setLoading(false);
    }
  };

  const handlePreview = async (file) => {
    // If already previewing this file, close it
    if (previewFile === file.filename) {
      setPreviewFile(null);
      setPreviewData(null);
      return;
    }

    // If previewing a different file, load new preview
    setPreviewLoading(true);
    setPreviewFile(file.filename);
    
    try {
      const response = await fetch(`http://127.0.0.1:5000/api/scraping/files/${file.filename}/preview`);
      const data = await response.json();
      
      if (data.status === 'success') {
        setPreviewData(data.data);
      } else {
        setError(`Failed to load preview: ${data.message}`);
        setPreviewFile(null);
      }
    } catch (err) {
      setError('Failed to load file preview');
      setPreviewFile(null);
      console.error('Error loading preview:', err);
    } finally {
      setPreviewLoading(false);
    }
  };

  const getFileIcon = (fileType) => {
    if (fileType === 'indices') {
      return '📊';
    } else if (fileType === 'cotations') {
      return '📈';
    } else {
      return '📄';
    }
  };

  const getFileColor = (fileType) => {
    if (fileType === 'indices') {
      return 'border-blue-400 bg-blue-400 bg-opacity-10';
    } else if (fileType === 'cotations') {
      return 'border-green-400 bg-green-400 bg-opacity-10';
    } else {
      return 'border-gray-400 bg-gray-400 bg-opacity-10';
    }
  };

  const formatFileSize = (sizeMB) => {
    if (sizeMB < 1) {
      return `${(sizeMB * 1024).toFixed(1)} KB`;
    }
    return `${sizeMB.toFixed(2)} MB`;
  };

  const formatDate = (dateString) => {
    return new Date(dateString).toLocaleString();
  };

  if (loading) {
    return (
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01 }}
      >
        <div className="flex items-center justify-center py-8">
          <motion.div
            className="w-8 h-8 border-2 border-light-green border-t-transparent rounded-full"
            animate={{ rotate: 360 }}
            transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
          ></motion.div>
          <span className="ml-3 text-crystal-white font-sans">Loading files...</span>
        </div>
      </motion.div>
    );
  }

  if (error) {
    return (
      <motion.div 
        className="bg-background-secondary rounded-lg shadow-lg border border-light-silver p-6 glass"
        variants={cardVariants}
        whileHover={{ scale: 1.01 }}
      >
        <div className="text-center py-8">
          <div className="text-red-400 text-4xl mb-4">❌</div>
          <h3 className="text-xl font-display font-semibold text-pure-white mb-2">Error Loading Files</h3>
          <p className="text-crystal-white font-sans">{error}</p>
        </div>
      </motion.div>
    );
  }

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
          <h3 className="text-xl font-display font-semibold text-pure-white">📁 Scraped Files</h3>
          <p className="text-crystal-white font-sans">
            {files.length} files available • Last updated: {lastUpdated ? formatDate(lastUpdated) : 'Never'}
          </p>
        </div>
        <motion.button
          onClick={fetchFiles}
          className="px-4 py-2 bg-crystal-white hover:bg-secondary-600 text-dark-bg font-display font-medium rounded-lg transition-all duration-300"
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
        >
          🔄 Refresh
        </motion.button>
      </motion.div>

      {/* Files Grid */}
      {files.length === 0 ? (
        <motion.div 
          className="text-center py-12"
          variants={itemVariants}
        >
          <div className="text-6xl mb-4">📂</div>
          <h4 className="text-lg font-display font-semibold text-pure-white mb-2">No Files Found</h4>
          <p className="text-crystal-white font-sans">Start scraping to generate CSV files</p>
        </motion.div>
      ) : (
        <motion.div 
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"
          variants={containerVariants}
        >
          {files.map((file, index) => (
            <motion.div
              key={file.filename || `file-${index}`}
              className={`bg-background-tertiary rounded-lg p-4 border ${getFileColor(file.file_type || 'unknown')} cursor-pointer transition-all duration-300`}
              variants={itemVariants}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={() => handlePreview(file)}
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center space-x-2">
                  <span className="text-2xl">{getFileIcon(file.file_type)}</span>
                  <div>
                    <h4 className="font-display font-semibold text-pure-white text-sm">
                      {file.file_type === 'indices' ? 'Indices' : 'Cotations'}
                    </h4>
                    <p className="text-xs text-crystal-white font-sans">{file.year || 'Unknown'}</p>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-xs font-display font-medium text-light-green">
                    {formatFileSize(file.size_mb || 0)}
                  </div>
                  <div className="text-xs text-pearl-white font-sans">
                    {(file.rows || 0).toLocaleString()} rows
                  </div>
                </div>
              </div>
              
              <div className="text-xs text-crystal-white font-sans mb-3">
                {file.filename || 'Unknown file'}
              </div>

              {/* Preview Toggle */}
              <motion.button
                className="w-full text-xs font-display font-medium text-crystal-white hover:text-light-green transition-colors duration-200"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                {previewFile === file.filename ? 'Hide Preview' : 'Show Preview'}
              </motion.button>

              {/* Preview Data */}
              {previewFile === file.filename && (
                <motion.div
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                  className="mt-3 pt-3 border-t border-light-silver"
                >
                  {previewLoading ? (
                    <div className="flex items-center justify-center py-4">
                      <motion.div
                        className="w-4 h-4 border-2 border-light-green border-t-transparent rounded-full"
                        animate={{ rotate: 360 }}
                        transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                      ></motion.div>
                    </div>
                  ) : previewData ? (
                    <div className="bg-background-primary rounded p-2 max-h-32 overflow-y-auto custom-scrollbar">
                      <pre className="text-xs font-mono text-crystal-white">
                        {JSON.stringify(previewData, null, 2)}
                      </pre>
                    </div>
                  ) : (
                    <p className="text-xs text-pearl-white font-sans">No preview available</p>
                  )}
                </motion.div>
              )}
            </motion.div>
          ))}
        </motion.div>
      )}
    </motion.div>
  );
}
