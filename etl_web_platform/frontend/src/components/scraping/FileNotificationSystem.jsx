import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import FileInspectionCard from './FileInspectionCard';

const notificationVariants = {
  hidden: { opacity: 0, y: 50, scale: 0.8 },
  visible: { 
    opacity: 1, 
    y: 0, 
    scale: 1,
    transition: {
      duration: 0.3,
      ease: "easeOut"
    }
  },
  exit: { 
    opacity: 0, 
    y: -50, 
    scale: 0.8,
    transition: {
      duration: 0.2
    }
  }
};

export default function FileNotificationSystem({ isRunning, createdFiles }) {
  const [notifications, setNotifications] = useState([]);
  const [expandedFiles, setExpandedFiles] = useState(new Set());

  // Track new files and create notifications
  useEffect(() => {
    console.log('🔍 FileNotificationSystem: createdFiles changed:', createdFiles?.length);
    console.log('🔍 FileNotificationSystem: current notifications:', notifications.length);
    
    if (createdFiles && createdFiles.length > 0) {
      const newFiles = createdFiles.filter(file => 
        !notifications.some(n => n.filename === file.filename)
      );
      
      console.log('🔍 FileNotificationSystem: new files found:', newFiles.length);
      
      if (newFiles.length > 0) {
        const newNotifications = newFiles.map(file => ({
          ...file,
          id: Date.now() + Math.random(),
          timestamp: new Date().toISOString()
        }));
        
        console.log('🔍 FileNotificationSystem: creating notifications for:', newNotifications.map(n => n.filename));
        
        setNotifications(prev => {
          const updated = [...prev, ...newNotifications];
          console.log('🔍 FileNotificationSystem: total notifications after update:', updated.length);
          return updated;
        });
        
        // Auto-remove notifications after 10 seconds
        setTimeout(() => {
          setNotifications(prev => {
            const filtered = prev.filter(n => !newNotifications.some(nn => nn.id === n.id));
            console.log('🔍 FileNotificationSystem: auto-removed notifications, remaining:', filtered.length);
            return filtered;
          });
        }, 10000);
      }
    }
  }, [createdFiles, notifications]);

  const removeNotification = (id) => {
    console.log('🔴 Removing notification:', id);
    setNotifications(prev => {
      const filtered = prev.filter(n => n.id !== id);
      console.log('🔴 Notifications after removal:', filtered.length);
      return filtered;
    });
  };

  const toggleFileExpansion = (filename) => {
    setExpandedFiles(prev => {
      const newSet = new Set(prev);
      if (newSet.has(filename)) {
        newSet.delete(filename);
      } else {
        newSet.add(filename);
      }
      return newSet;
    });
  };

  if (!isRunning && notifications.length === 0) {
    return null;
  }

  return (
    <div className="fixed bottom-4 right-4 z-50 space-y-3 max-w-sm">
      {/* Active Scraping Indicator */}
      <AnimatePresence>
        {isRunning && (
          <motion.div 
            className="bg-background-secondary border border-light-silver text-pure-white p-4 rounded-lg shadow-lg glass"
            variants={notificationVariants}
            initial="hidden"
            animate="visible"
            exit="exit"
          >
            <div className="flex items-center space-x-3">
              <motion.span 
                className="text-2xl"
                animate={{ rotate: 360 }}
                transition={{ duration: 2, repeat: Infinity, ease: "linear" }}
              >
                🔄
              </motion.span>
              <div>
                <p className="font-display font-medium text-pure-white">Scraping in Progress</p>
                <p className="text-sm text-crystal-white font-sans">Creating CSV files...</p>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* File Creation Notifications */}
      <AnimatePresence>
        {notifications.map((notification) => (
          <motion.div
            key={notification.id}
            className="bg-background-secondary border border-light-green rounded-lg shadow-lg glass"
            variants={notificationVariants}
            initial="hidden"
            animate="visible"
            exit="exit"
            whileHover={{ scale: 1.02 }}
          >
            {/* Notification Header */}
            <div className="p-4 border-b border-light-silver">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <span className="text-2xl">📁</span>
                  <span className="font-display font-medium text-light-green">New File Created!</span>
                </div>
                <motion.button
                  onClick={() => removeNotification(notification.id)}
                  className="text-pearl-white hover:text-red-400 transition-colors duration-200"
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                >
                  ✕
                </motion.button>
              </div>
            </div>

            {/* File Details */}
            <div className="p-4">
              <div className="mb-3">
                <h4 className="font-display font-semibold text-pure-white text-sm mb-1">
                  {notification.filename}
                </h4>
                <div className="flex items-center justify-between text-xs text-crystal-white font-sans">
                  <span>{notification.file_type === 'indices' ? '📊 Indices' : '📈 Cotations'}</span>
                  <span>{notification.year}</span>
                </div>
              </div>

              {/* Expandable Content */}
              <motion.button
                onClick={() => toggleFileExpansion(notification.filename)}
                className="w-full text-xs font-display font-medium text-crystal-white hover:text-light-green transition-colors duration-200 flex items-center justify-between"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <span>View Details</span>
                <motion.span
                  animate={{ rotate: expandedFiles.has(notification.filename) ? 180 : 0 }}
                  transition={{ duration: 0.2 }}
                >
                  ▼
                </motion.span>
              </motion.button>

              <AnimatePresence>
                {expandedFiles.has(notification.filename) && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: "auto", opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.3 }}
                    className="mt-3 pt-3 border-t border-light-silver"
                  >
                    <FileInspectionCard file={notification} />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}
