import React from 'react';
import { motion } from 'framer-motion';

const Loader = () => {
  const containerVariants = {
    start: {
      transition: {
        staggerChildren: 0.2,
      },
    },
    end: {
      transition: {
        staggerChildren: 0.2,
      },
    },
  };

  const circleVariants = {
    start: {
      y: "50%",
    },
    end: {
      y: "150%",
    },
  };

  const circleTransition = {
    duration: 0.5,
    repeat: Infinity,
    repeatType: "reverse",
    ease: "easeInOut",
  };

  const barVariants = {
    initial: { scaleY: 0.3 },
    animate: { scaleY: [0.3, 1, 0.3] },
  };

  return (
    <div className="fixed inset-0 bg-dark-bg z-50 flex items-center justify-center">
      {/* Background pattern */}
      <div className="absolute inset-0 opacity-10">
        <div className="absolute inset-0 bg-gradient-to-br from-light-green/20 to-transparent"></div>
        {[...Array(20)].map((_, i) => (
          <motion.div
            key={i}
            className="absolute w-1 h-1 bg-light-green/30 rounded-full"
            style={{
              left: `${Math.random() * 100}%`,
              top: `${Math.random() * 100}%`,
            }}
            animate={{
              opacity: [0.3, 1, 0.3],
              scale: [1, 1.5, 1],
            }}
            transition={{
              duration: 2 + Math.random() * 2,
              repeat: Infinity,
              delay: Math.random() * 2,
            }}
          />
        ))}
      </div>

      <div className="text-center">
        {/* Main loader animation */}
        <div className="flex flex-col items-center mb-8">
          {/* Financial chart loader */}
          <div className="flex items-end justify-center space-x-1 mb-6">
            {[...Array(8)].map((_, i) => (
              <motion.div
                key={i}
                className="w-3 bg-gradient-to-t from-light-green to-primary-400 rounded-t-sm"
                style={{ height: `${20 + Math.random() * 40}px` }}
                variants={barVariants}
                initial="initial"
                animate="animate"
                transition={{
                  duration: 1.5,
                  repeat: Infinity,
                  delay: i * 0.1,
                  ease: "easeInOut",
                }}
              />
            ))}
          </div>

          {/* Bouncing dots */}
          <motion.div
            className="flex space-x-2"
            variants={containerVariants}
            initial="start"
            animate="end"
          >
            {[...Array(3)].map((_, index) => (
              <motion.div
                key={index}
                className="w-3 h-3 bg-light-green rounded-full"
                variants={circleVariants}
                transition={circleTransition}
              />
            ))}
          </motion.div>
        </div>

        {/* Loading text */}
        <motion.div
          className="text-center"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
        >
          <motion.h2
            className="text-2xl font-serif font-bold text-pure-white mb-2"
            animate={{
              opacity: [0.7, 1, 0.7],
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: "easeInOut",
            }}
          >
            BVMT Intelligent Analytics
          </motion.h2>
          <motion.p
            className="text-light-green text-lg"
            animate={{
              opacity: [0.5, 1, 0.5],
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: "easeInOut",
              delay: 0.5,
            }}
          >
            Loading financial insights...
          </motion.p>
        </motion.div>

        {/* Progress bar */}
        <motion.div
          className="w-64 h-1 bg-background-secondary rounded-full overflow-hidden mt-8"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1 }}
        >
          <motion.div
            className="h-full bg-gradient-to-r from-light-green to-primary-400"
            initial={{ width: "0%" }}
            animate={{ width: "100%" }}
            transition={{
              duration: 3,
              repeat: Infinity,
              ease: "easeInOut",
            }}
          />
        </motion.div>

        {/* Rotating financial symbol */}
        <motion.div
          className="mt-6 flex justify-center"
          animate={{ rotate: 360 }}
          transition={{
            duration: 4,
            repeat: Infinity,
            ease: "linear",
          }}
        >
          <div className="w-12 h-12 border-2 border-light-green border-dashed rounded-full flex items-center justify-center">
            <span className="text-light-green font-bold text-lg">₹</span>
          </div>
        </motion.div>
      </div>
    </div>
  );
};

export default Loader;
