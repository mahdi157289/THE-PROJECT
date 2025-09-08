import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Link } from 'react-router-dom';
import bvmtLogo from '../assets/bvmt-logo.png';
import reactLogo from '../assets/react-logo.svg';
import pythonLogo from '../assets/icons8-python-100.png';
import powerbiLogo from '../assets/icons8-power-bi-2021-100.png';
import postgresqlLogo from '../assets/icons8-postgre-sql-a-free-and-open-source-relational-database-management-system-100.png';
import flaskLogo from '../assets/icons8-flask-100.png';
import dockerLogo from '../assets/icons8-docker-a-set-of-coupled-software-as-a-service-100.png';
import tailwindLogo from '../assets/icons8-tailwindcss-100.png';
import framerLogo from '../assets/icons8-alight-motion-100.png';
import { 
  HiPlay, 
  HiChartBar, 
  HiCog, 
  HiShieldCheck, 
  HiLightningBolt, 
  HiGlobe,
  HiDatabase,
  HiCube,
  HiSparkles,
  HiTrendingUp,
  HiEye,
  HiUsers,
  HiArrowRight,
  HiMenu,
  HiX
} from 'react-icons/hi';

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
      duration: 0.6,
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
      duration: 0.5,
      ease: "easeOut"
    }
  }
};

export default function Welcome() {
  const [activeSection, setActiveSection] = useState('hero');
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  // Navigation sections
  const sections = [
    { id: 'hero', label: 'Home', icon: HiGlobe },
    { id: 'features', label: 'Features', icon: HiSparkles },
    { id: 'demo', label: 'Demo', icon: HiPlay },
    { id: 'tech', label: 'Tech Stack', icon: HiCube },
    { id: 'highlights', label: 'Highlights', icon: HiLightningBolt },
    { id: 'start', label: 'Get Started', icon: HiArrowRight }
  ];

  // Scroll spy effect
  useEffect(() => {
    const handleScroll = () => {
      const scrollPosition = window.scrollY + 100;
      
      sections.forEach(section => {
        const element = document.getElementById(section.id);
        if (element) {
          const { offsetTop, offsetHeight } = element;
          if (scrollPosition >= offsetTop && scrollPosition < offsetTop + offsetHeight) {
            setActiveSection(section.id);
          }
        }
      });
    };

    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);


  // Smooth scroll to section
  const scrollToSection = (sectionId) => {
    const element = document.getElementById(sectionId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' });
    }
    setMobileMenuOpen(false);
  };

  // Static data
  const features = [
    {
      icon: HiDatabase,
      title: "Intelligent Data Architecture",
      description: "Bronze, Silver, Golden, and Diamond layers with AI-enhanced processing",
      demo: "Interactive pipeline visualization"
    },
    {
      icon: HiTrendingUp,
      title: "Predictive Analytics",
      description: "Advanced AI models for BVMT stock market predictions and insights",
      demo: "Live intelligent dashboard"
    },
    {
      icon: HiCog,
      title: "Smart Processing",
      description: "AI-powered stream processing with Apache Spark for intelligent insights",
      demo: "Real-time smart analytics"
    },
    {
      icon: HiShieldCheck,
      title: "Intelligent Security",
      description: "AI-driven threat detection, smart authentication, and adaptive security",
      demo: "Security intelligence features"
    }
  ];

  const techStack = [
    { name: "React", category: "Frontend", logo: reactLogo },
    { name: "Python", category: "Backend", logo: pythonLogo },
    { name: "Power BI", category: "Analytics", logo: powerbiLogo },
    { name: "PostgreSQL", category: "Database", logo: postgresqlLogo },
    { name: "Flask", category: "API", logo: flaskLogo },
    { name: "Docker", category: "DevOps", logo: dockerLogo },
    { name: "Tailwind CSS", category: "Styling", logo: tailwindLogo },
    { name: "Framer Motion", category: "Animation", logo: framerLogo }
  ];

  const highlights = [
    {
      icon: HiLightningBolt,
      title: "Real-time Processing",
      value: "< 100ms",
      description: "Ultra-fast data processing and response times"
    },
    {
      icon: HiShieldCheck,
      title: "Security First",
      value: "99.9%",
      description: "Enterprise-grade security and data protection"
    },
    {
      icon: HiTrendingUp,
      title: "Data Accuracy",
      value: "99.95%",
      description: "Validated financial data with comprehensive quality checks"
    },
    {
      icon: HiUsers,
      title: "Scalability",
      value: "1M+",
      description: "Records processed daily with horizontal scaling"
    }
  ];

  return (
    <div className="min-h-screen bg-dark-bg">
      {/* Enhanced Navigation Bar */}
      <motion.nav 
        className="fixed top-0 w-full z-50 bg-gradient-to-r from-background-secondary via-background-primary to-background-secondary bg-opacity-95 backdrop-blur-xl border-b border-light-green border-opacity-30 shadow-2xl"
        initial={{ y: -100, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        transition={{ duration: 0.8, ease: "easeOut" }}
        style={{
          borderRadius: "0 0 24px 24px"
        }}
      >
        {/* Animated background glow */}
        <motion.div
          className="absolute inset-0 bg-gradient-to-r from-light-green to-primary-600 opacity-5"
          animate={{
            backgroundPosition: ["0% 50%", "100% 50%", "0% 50%"],
          }}
          transition={{
            duration: 8,
            repeat: Infinity,
            ease: "linear"
          }}
          style={{
            borderRadius: "0 0 24px 24px"
          }}
        />
        
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 relative">
          <div className="flex justify-between items-center h-20">
            {/* Left Navigation */}
            <div className="hidden lg:flex items-center space-x-6 flex-1">
              {sections.slice(0, 3).map((section) => {
                const Icon = section.icon;
                return (
                  <motion.button
                    key={section.id}
                    onClick={() => scrollToSection(section.id)}
                    className={`group flex items-center px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${
                      activeSection === section.id
                        ? 'text-light-green bg-light-green bg-opacity-20 shadow-lg'
                        : 'text-crystal-white hover:text-light-green'
                    }`}
                    whileHover={{ scale: 1.1, y: -2 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <motion.div
                      className="mr-2"
                      whileHover={{ rotate: 360 }}
                      transition={{ duration: 0.6 }}
                    >
                      <Icon className="w-4 h-4" />
                    </motion.div>
                    {section.label}
                    {activeSection === section.id && (
                      <motion.div
                        className="absolute bottom-0 left-0 right-0 h-0.5 bg-light-green rounded-full"
                        layoutId="activeSection"
                        transition={{ duration: 0.3 }}
                      />
                    )}
                  </motion.button>
                );
              })}
            </div>

            {/* Center Logo */}
            <motion.div 
              className="flex items-center justify-center"
              initial={{ scale: 0, rotate: -180 }}
              animate={{ scale: 1, rotate: 0 }}
              transition={{ 
                delay: 0.3, 
                duration: 0.8, 
                type: "spring", 
                stiffness: 200 
              }}
              whileHover={{ scale: 1.1 }}
            >
              <motion.div
                className="relative group cursor-pointer"
                whileHover={{ scale: 1.05 }}
              >
                {/* Glow effect */}
                <motion.div
                  className="absolute -inset-2 bg-gradient-to-r from-light-green to-primary-600 rounded-full opacity-20 group-hover:opacity-40 blur-xl"
                  animate={{
                    scale: [1, 1.2, 1],
                    opacity: [0.2, 0.4, 0.2]
                  }}
                  transition={{
                    duration: 3,
                    repeat: Infinity,
                    ease: "easeInOut"
                  }}
                />
                
                 {/* Logo container */}
                 <motion.div
                   className="relative bg-gradient-to-br from-background-tertiary to-background-secondary p-4 rounded-2xl border border-light-green border-opacity-30 shadow-2xl"
                   whileHover={{
                     boxShadow: "0 25px 50px rgba(144, 238, 144, 0.3)",
                     borderColor: "rgba(144, 238, 144, 0.8)"
                   }}
                 >
                   <img
                     src={bvmtLogo}
                     alt="BVMT - Bourse des Valeurs Mobilières de Tunis"
                     className="w-12 h-12 object-contain"
                     onError={(e) => {
                       // Fallback to Database icon if image fails to load
                       e.target.style.display = 'none';
                       e.target.nextSibling.style.display = 'flex';
                     }}
                   />
                   <motion.div
                     className="w-12 h-12 bg-light-green rounded-lg items-center justify-center hidden"
                     whileHover={{ rotate: 360 }}
                     transition={{ duration: 0.6 }}
                   >
                     <HiDatabase className="w-8 h-8 text-dark-bg" />
                   </motion.div>
                 </motion.div>
                
              </motion.div>
            </motion.div>

            {/* Right Navigation */}
            <div className="hidden lg:flex items-center space-x-6 flex-1 justify-end">
              {sections.slice(3).map((section) => {
                const Icon = section.icon;
                return (
                  <motion.button
                    key={section.id}
                    onClick={() => scrollToSection(section.id)}
                    className={`group flex items-center px-4 py-2 rounded-xl text-sm font-medium transition-all duration-300 ${
                      activeSection === section.id
                        ? 'text-light-green bg-light-green bg-opacity-20 shadow-lg'
                        : 'text-crystal-white hover:text-light-green'
                    }`}
                    whileHover={{ scale: 1.1, y: -2 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <motion.div
                      className="mr-2"
                      whileHover={{ rotate: 360 }}
                      transition={{ duration: 0.6 }}
                    >
                      <Icon className="w-4 h-4" />
                    </motion.div>
                    {section.label}
                    {activeSection === section.id && (
                      <motion.div
                        className="absolute bottom-0 left-0 right-0 h-0.5 bg-light-green rounded-full"
                        layoutId="activeSection2"
                        transition={{ duration: 0.3 }}
                      />
                    )}
                  </motion.button>
                );
              })}
            </div>

            {/* Mobile/Tablet Navigation */}
            <div className="lg:hidden flex items-center space-x-4">
              {/* Compact logo for mobile */}
              <motion.div
                className="flex items-center"
                whileHover={{ scale: 1.05 }}
              >
                <motion.div
                  className="w-10 h-10 bg-gradient-to-br from-light-green to-primary-600 rounded-xl flex items-center justify-center mr-2"
                  whileHover={{ rotate: 360 }}
                  transition={{ duration: 0.6 }}
                >
                  <HiDatabase className="w-6 h-6 text-dark-bg" />
                </motion.div>
                <span className="text-lg font-serif font-bold text-pure-white">BVMT</span>
              </motion.div>

              {/* Mobile menu button */}
              <motion.button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="relative p-3 text-crystal-white hover:text-light-green bg-background-secondary rounded-xl border border-light-green border-opacity-30"
                whileHover={{ scale: 1.1, boxShadow: "0 10px 20px rgba(144, 238, 144, 0.2)" }}
                whileTap={{ scale: 0.95 }}
              >
                <motion.div
                  animate={{ rotate: mobileMenuOpen ? 180 : 0 }}
                  transition={{ duration: 0.3 }}
                >
                  {mobileMenuOpen ? <HiX className="w-6 h-6" /> : <HiMenu className="w-6 h-6" />}
                </motion.div>
              </motion.button>
            </div>
          </div>
        </div>

        {/* Enhanced Mobile Navigation */}
        <AnimatePresence>
          {mobileMenuOpen && (
            <motion.div 
              className="lg:hidden bg-gradient-to-br from-background-secondary to-background-tertiary border-t border-light-green border-opacity-30 backdrop-blur-xl"
              initial={{ opacity: 0, height: 0, y: -20 }}
              animate={{ opacity: 1, height: 'auto', y: 0 }}
              exit={{ opacity: 0, height: 0, y: -20 }}
              transition={{ duration: 0.4, ease: "easeOut" }}
            >
              <div className="px-4 pt-4 pb-6 space-y-3">
                {sections.map((section, index) => {
                  const Icon = section.icon;
                  return (
                    <motion.button
                      key={section.id}
                      onClick={() => scrollToSection(section.id)}
                      className={`flex items-center w-full px-4 py-3 rounded-xl text-sm font-medium transition-all duration-300 ${
                        activeSection === section.id
                          ? 'text-light-green bg-light-green bg-opacity-20 shadow-lg border border-light-green border-opacity-30'
                          : 'text-crystal-white hover:text-light-green hover:bg-light-green hover:bg-opacity-10 border border-transparent hover:border-light-green hover:border-opacity-20'
                      }`}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index * 0.1 }}
                      whileHover={{ scale: 1.02, x: 5 }}
                      whileTap={{ scale: 0.98 }}
                    >
                      <motion.div
                        className="mr-3"
                        whileHover={{ rotate: 360 }}
                        transition={{ duration: 0.6 }}
                      >
                        <Icon className="w-5 h-5" />
                      </motion.div>
                      {section.label}
                      {activeSection === section.id && (
                        <motion.div
                          className="ml-auto w-2 h-2 bg-light-green rounded-full"
                          initial={{ scale: 0 }}
                          animate={{ scale: 1 }}
                          transition={{ duration: 0.3 }}
                        />
                      )}
                    </motion.button>
                  );
                })}
                
                {/* Mobile CTA Buttons */}
                <motion.div 
                  className="pt-4 mt-4 border-t border-light-green border-opacity-20"
                  initial={{ opacity: 0, y: 20 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: 0.3 }}
                >
                  <div className="flex flex-col space-y-3">
                     <Link to="/register">
                       <motion.button
                         className="w-full px-4 py-3 bg-light-green text-dark-bg font-medium rounded-xl hover:bg-primary-600 transition-all duration-300"
                         whileHover={{ scale: 1.02 }}
                         whileTap={{ scale: 0.98 }}
                         onClick={() => setMobileMenuOpen(false)}
                       >
                         Sign Up
                       </motion.button>
                     </Link>
                    <Link to="/register">
                      <motion.button
                        className="w-full px-4 py-3 border border-light-green text-light-green font-medium rounded-xl hover:bg-light-green hover:text-dark-bg transition-all duration-300"
                        whileHover={{ scale: 1.02 }}
                        whileTap={{ scale: 0.98 }}
                        onClick={() => setMobileMenuOpen(false)}
                      >
                        Register
                      </motion.button>
                    </Link>
                  </div>
                </motion.div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </motion.nav>

      {/* Hero Section */}
      <section id="hero" className="min-h-screen flex items-center justify-center pt-24">
        <motion.div 
          className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center"
          variants={containerVariants}
          initial="hidden"
          animate="visible"
        >
           <motion.div variants={itemVariants}>
             <motion.div
               className="w-40 h-40 bg-gradient-to-br from-background-tertiary to-background-secondary rounded-full flex items-center justify-center mx-auto mb-8 border-4 border-light-green border-opacity-30 shadow-2xl"
               whileHover={{ scale: 1.1, rotate: 360 }}
               transition={{ duration: 0.8 }}
             >
               <motion.div
                 className="relative"
                 whileHover={{ scale: 1.1 }}
               >
                 <img
                   src={bvmtLogo}
                   alt="BVMT - Bourse des Valeurs Mobilières de Tunis"
                   className="w-32 h-32 object-contain rounded-full"
                   onError={(e) => {
                     // Fallback to Database icon if image fails to load
                     e.target.style.display = 'none';
                     e.target.nextSibling.style.display = 'flex';
                   }}
                 />
                 <motion.div
                   className="w-32 h-32 bg-light-green rounded-full items-center justify-center hidden"
                   whileHover={{ rotate: 360 }}
                   transition={{ duration: 0.6 }}
                 >
                   <HiDatabase className="w-20 h-20 text-dark-bg" />
                 </motion.div>
               </motion.div>
             </motion.div>
            
            <h1 className="text-6xl md:text-8xl font-serif font-bold text-pure-white mb-6">
              <motion.span
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.2, duration: 0.8 }}
              >
                BVMT
              </motion.span>
              <br />
              <motion.span
                className="text-transparent bg-clip-text bg-gradient-to-r from-light-green to-primary-600"
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.4, duration: 0.8 }}
              >
                Intelligent Analytics
              </motion.span>
            </h1>
            
            <motion.p 
              className="text-xl md:text-2xl text-crystal-white mb-8 max-w-4xl mx-auto"
              variants={itemVariants}
            >
              Advanced Medallion Architecture with Intelligent Analytics for Tunisian Financial Market
              <br />
              <span className="text-light-green font-medium">AI-Powered Insights • Predictive Analytics • Smart Decision Making</span>
            </motion.p>

            {/* Animated Line */}
            <motion.div 
              className="flex justify-center my-8"
              variants={itemVariants}
            >
              <motion.div 
                className="w-96 h-1 bg-gradient-to-r from-transparent via-light-green to-transparent"
                initial={{ scaleX: 0 }}
                animate={{ scaleX: 1 }}
                transition={{ delay: 0.8, duration: 1 }}
              />
            </motion.div>

            {/* CTA Buttons */}
            <motion.div 
              className="flex flex-col sm:flex-row gap-4 justify-center"
              variants={itemVariants}
            >
              <motion.button
                onClick={() => scrollToSection('demo')}
                className="px-8 py-4 bg-light-green text-dark-bg font-display font-semibold rounded-lg text-lg hover:bg-primary-600 transition-all duration-300"
                whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.3)" }}
                whileTap={{ scale: 0.95 }}
              >
                <HiPlay className="inline w-5 h-5 mr-2" />
                Explore Demo
              </motion.button>
              
               <Link to="/register">
                 <motion.button
                   className="px-8 py-4 border-2 border-light-green text-light-green font-display font-semibold rounded-lg text-lg hover:bg-light-green hover:text-dark-bg transition-all duration-300"
                   whileHover={{ scale: 1.05 }}
                   whileTap={{ scale: 0.95 }}
                 >
                   <HiArrowRight className="inline w-5 h-5 mr-2" />
                   Get Started
                 </motion.button>
               </Link>
            </motion.div>
          </motion.div>

          {/* Financial Flash Effects */}
          <div className="absolute inset-0 overflow-hidden pointer-events-none">
            {/* Price Flash Indicators */}
            {[...Array(12)].map((_, i) => {
              const isPositive = Math.random() > 0.5;
              const flashTypes = ['▲', '▼', '●', '♦', '■'];
              const symbol = flashTypes[Math.floor(Math.random() * flashTypes.length)];
              
              return (
                <motion.div
                  key={i}
                  className={`absolute text-lg font-bold ${
                    isPositive ? 'text-green-400' : 'text-red-400'
                  }`}
                  initial={{ 
                    opacity: 0, 
                    scale: 0.5,
                    x: Math.random() * window.innerWidth || 1200,
                    y: Math.random() * window.innerHeight || 800
                  }}
                  animate={{
                    opacity: [0, 1, 1, 0],
                    scale: [0.5, 1.2, 1, 0.8],
                    y: [0, -30, -60, -90],
                  }}
                  transition={{
                    duration: 2 + Math.random() * 1.5,
                    repeat: Infinity,
                    delay: Math.random() * 3,
                    ease: "easeOut"
                  }}
                  style={{
                    left: `${Math.random() * 100}%`,
                    top: `${60 + Math.random() * 30}%` // Keep in lower portion
                  }}
                >
                  {symbol}
                </motion.div>
              );
            })}

            {/* Trading Volume Flashes */}
            {[...Array(8)].map((_, i) => (
              <motion.div
                key={`volume-${i}`}
                className="absolute"
                initial={{ opacity: 0 }}
                animate={{
                  opacity: [0, 0.8, 0.8, 0],
                  scale: [0.8, 1.2, 1, 0.9],
                }}
                transition={{
                  duration: 1.5 + Math.random() * 1,
                  repeat: Infinity,
                  delay: Math.random() * 4,
                  ease: "easeInOut"
                }}
                style={{
                  left: `${Math.random() * 100}%`,
                  top: `${Math.random() * 100}%`
                }}
              >
                <div className="bg-light-green bg-opacity-30 rounded-full w-3 h-3 blur-sm"></div>
                <div className="absolute inset-0 bg-light-green bg-opacity-60 rounded-full w-1 h-1 m-1"></div>
              </motion.div>
            ))}

            {/* Market Data Streams */}
            {[...Array(6)].map((_, i) => {
              const prices = ['₪1,234', '€567.89', '$89.12', '¥4,567', '£234.56', '₿0.045'];
              const price = prices[Math.floor(Math.random() * prices.length)];
              
              return (
                <motion.div
                  key={`price-${i}`}
                  className="absolute text-xs font-mono text-crystal-white bg-background-secondary bg-opacity-70 px-2 py-1 rounded border border-light-green border-opacity-30"
                  initial={{ 
                    opacity: 0,
                    x: window.innerWidth || 1200,
                  }}
                  animate={{
                    opacity: [0, 1, 1, 0],
                    x: [100, -100],
                    y: [0, Math.random() * 20 - 10], // Slight vertical drift
                  }}
                  transition={{
                    duration: 8 + Math.random() * 4,
                    repeat: Infinity,
                    delay: Math.random() * 6,
                    ease: "linear"
                  }}
                  style={{
                    top: `${20 + Math.random() * 60}%`
                  }}
                >
                  {price}
                </motion.div>
              );
            })}

            {/* Candlestick Flash Effects */}
            {[...Array(10)].map((_, i) => {
              const isGreen = Math.random() > 0.4;
              
              return (
                <motion.div
                  key={`candle-${i}`}
                  className="absolute"
                  initial={{ opacity: 0, scaleY: 0 }}
                  animate={{
                    opacity: [0, 0.7, 0.7, 0],
                    scaleY: [0, 1, 1, 0],
                    scaleX: [0.5, 1, 1, 0.5],
                  }}
                  transition={{
                    duration: 1.8 + Math.random() * 1.2,
                    repeat: Infinity,
                    delay: Math.random() * 5,
                    ease: "easeInOut"
                  }}
                  style={{
                    left: `${Math.random() * 100}%`,
                    top: `${Math.random() * 100}%`,
                    width: '3px',
                    height: `${10 + Math.random() * 20}px`,
                  }}
                >
                  {/* Candlestick body */}
                  <div 
                    className={`w-full h-full ${
                      isGreen 
                        ? 'bg-green-400 border border-green-300' 
                        : 'bg-red-400 border border-red-300'
                    } opacity-60`}
                  />
                  
                  {/* Candlestick wick */}
                  <div 
                    className={`absolute left-1/2 transform -translate-x-1/2 w-px ${
                      isGreen ? 'bg-green-400' : 'bg-red-400'
                    }`}
                    style={{
                      height: `${5 + Math.random() * 10}px`,
                      top: `-${2 + Math.random() * 5}px`
                    }}
                  />
                </motion.div>
              );
            })}

            {/* Quick Flash Alerts */}
            {[...Array(15)].map((_, i) => (
              <motion.div
                key={`flash-${i}`}
                className="absolute w-1 h-1 rounded-full"
                initial={{ 
                  opacity: 0,
                  scale: 0,
                  backgroundColor: Math.random() > 0.5 ? '#4ade80' : '#f87171'
                }}
                animate={{
                  opacity: [0, 1, 0],
                  scale: [0, 1.5, 0],
                }}
                transition={{
                  duration: 0.6 + Math.random() * 0.4,
                  repeat: Infinity,
                  delay: Math.random() * 3,
                  ease: "easeOut"
                }}
                style={{
                  left: `${Math.random() * 100}%`,
                  top: `${Math.random() * 100}%`,
                  boxShadow: '0 0 10px currentColor'
                }}
              />
            ))}
          </div>
        </motion.div>
      </section>

      {/* Seamless Video Integration with Company Introduction */}
      <section className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-background-primary via-background-secondary to-background-primary opacity-20 pointer-events-none"></div>
        
        <video 
          autoPlay
          muted
          loop
          playsInline
          className="w-full h-auto object-cover"
          style={{ 
            filter: 'brightness(0.7) contrast(1.1)',
            mixBlendMode: 'screen'
          }}
        >
          <source src="/videos/Solid Logo Reveal_free.mp4" type="video/mp4" />
        </video>
        
        {/* Company Introduction Overlay */}
        <div className="absolute inset-0 flex items-center justify-end bg-gradient-to-t from-background-primary/90 via-transparent to-background-primary/70">
          <motion.div 
            className="max-w-2xl mr-8 md:mr-16 lg:mr-20 px-4 sm:px-6 lg:px-8 text-right"
            initial={{ opacity: 0, x: 100, scale: 0.9 }}
            animate={{ 
              opacity: [0, 1, 1, 1],
              x: [100, 0, 5, 0],
              scale: [0.9, 1, 1.02, 1]
            }}
            transition={{ 
              duration: 3,
              times: [0, 0.3, 0.7, 1],
              ease: "easeInOut"
            }}
          >
            <motion.h2 
              className="text-3xl md:text-4xl lg:text-5xl font-serif font-bold text-pure-white mb-2"
              initial={{ opacity: 0, x: 50, rotateY: 15 }}
              animate={{ 
                opacity: [0, 0, 1, 1],
                x: [50, 50, 0, 0],
                rotateY: [15, 15, 0, 0]
              }}
              transition={{ 
                duration: 4,
                times: [0, 0.2, 0.6, 1],
                ease: "easeOut"
              }}
            >
              BVMT
            </motion.h2>
            
            <motion.h3 
              className="text-xl md:text-2xl lg:text-3xl font-serif text-light-green mb-6"
              initial={{ opacity: 0, x: 30 }}
              animate={{ 
                opacity: [0, 0, 0, 1],
                x: [30, 20, 10, 0]
              }}
              transition={{ 
                duration: 4.5,
                times: [0, 0.3, 0.7, 1],
                ease: "easeOut"
              }}
              style={{ direction: 'rtl' }}
            >
              بورصة تونس للأوراق المالية
            </motion.h3>
            
            <motion.div 
              className="w-24 h-0.5 bg-light-green ml-auto mb-8"
              initial={{ scaleX: 0, opacity: 0 }}
              animate={{ 
                scaleX: [0, 0, 1, 1],
                opacity: [0, 0, 1, 1]
              }}
              transition={{ 
                duration: 4,
                times: [0, 0.4, 0.8, 1],
                ease: "easeInOut"
              }}
            />
            
            <motion.p 
              className="text-lg md:text-xl text-crystal-white leading-relaxed mb-8"
              initial={{ opacity: 0, x: 80, filter: "blur(10px)" }}
              animate={{ 
                opacity: [0, 0, 0, 1],
                x: [80, 60, 20, 0],
                filter: ["blur(10px)", "blur(8px)", "blur(2px)", "blur(0px)"]
              }}
              transition={{ 
                duration: 5,
                times: [0, 0.3, 0.7, 1],
                ease: "easeOut"
              }}
              style={{ direction: 'rtl', textAlign: 'right' }}
            >
              بورصة تونس للأوراق المالية هي المؤسسة الرائدة في تونس لتداول الأوراق المالية والاستثمار. 
              تأسست لتكون المحرك الرئيسي للاقتصاد التونسي من خلال توفير منصة آمنة وشفافة للمستثمرين 
              والشركات لتداول الأسهم والسندات وتعزيز النمو الاقتصادي المستدام.
            </motion.p>
            
            <motion.p 
              className="text-base md:text-lg text-crystal-white/80 leading-relaxed"
              initial={{ opacity: 0, y: 30, scale: 0.95 }}
              animate={{ 
                opacity: [0, 0, 0, 0, 1],
                y: [30, 20, 10, 5, 0],
                scale: [0.95, 0.97, 0.99, 1, 1]
              }}
              transition={{ 
                duration: 6,
                times: [0, 0.2, 0.5, 0.8, 1],
                ease: "easeOut"
              }}
              style={{ direction: 'rtl', textAlign: 'right' }}
            >
              منصتنا الذكية للتحليلات المالية تجمع بين تقنيات الذكاء الاصطناعي والبيانات الضخمة 
              لتقديم رؤى استثمارية دقيقة ومتطورة. نحن نحول البيانات المالية المعقدة إلى معلومات 
              قابلة للفهم والتطبيق، مما يمكن المستثمرين من اتخاذ قرارات مدروسة وذكية في عالم 
              الأسواق المالية المتغير بسرعة.
            </motion.p>
          </motion.div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="py-20 bg-background-secondary">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div 
            className="text-center mb-16"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <motion.h2 
              className="text-4xl md:text-5xl font-serif font-bold text-pure-white mb-6"
              variants={itemVariants}
            >
              Platform Capabilities
            </motion.h2>
            <motion.p 
              className="text-xl text-crystal-white max-w-3xl mx-auto"
              variants={itemVariants}
            >
              Comprehensive intelligent analytics with cutting-edge AI technology
            </motion.p>
            
            <motion.div 
              className="flex justify-center my-8"
              variants={itemVariants}
            >
              <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent" />
            </motion.div>
          </motion.div>

          <motion.div 
            className="grid grid-cols-1 md:grid-cols-2 gap-8"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            {features.map((feature, index) => {
              const Icon = feature.icon;
              return (
                <motion.div
                  key={index}
                  className="bg-background-tertiary rounded-lg border border-light-silver p-8 glass"
                  variants={cardVariants}
                  whileHover={{ 
                    scale: 1.05, 
                    boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)",
                    borderColor: "rgba(144, 238, 144, 0.5)"
                  }}
                  transition={{ duration: 0.3 }}
                >
                  <div className="flex items-center mb-6">
                    <motion.div
                      className="w-12 h-12 bg-light-green rounded-lg flex items-center justify-center mr-4"
                      whileHover={{ rotate: 360 }}
                      transition={{ duration: 0.6 }}
                    >
                      <Icon className="w-6 h-6 text-dark-bg" />
                    </motion.div>
                    <h3 className="text-xl font-serif font-semibold text-pure-white">{feature.title}</h3>
                  </div>
                  
                  <p className="text-crystal-white mb-4">{feature.description}</p>
                  
                   <Link to="/login">
                     <motion.button
                       className="text-light-green font-medium hover:text-primary-600 transition-colors duration-200 flex items-center"
                       whileHover={{ x: 5 }}
                     >
                       <HiEye className="w-4 h-4 mr-2" />
                       View {feature.demo}
                     </motion.button>
                   </Link>
                </motion.div>
              );
            })}
          </motion.div>
        </div>
      </section>

      {/* Demo Section */}
      <section id="demo" className="py-20 bg-background-primary">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div 
            className="text-center mb-16"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <motion.h2 
              className="text-4xl md:text-5xl font-serif font-bold text-pure-white mb-6"
              variants={itemVariants}
            >
              Interactive Demos
            </motion.h2>
            <motion.p 
              className="text-xl text-crystal-white max-w-3xl mx-auto"
              variants={itemVariants}
            >
              Experience the power of intelligent analytics through live demonstrations
            </motion.p>
          </motion.div>

          <motion.div 
            className="grid grid-cols-1 lg:grid-cols-2 gap-8"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            {/* ETL Pipeline Demo */}
            <motion.div
              className="bg-background-secondary rounded-lg border border-light-silver p-8 glass flex flex-col h-full"
              variants={cardVariants}
              whileHover={{ scale: 1.02 }}
            >
              <div className="flex items-center mb-6">
                <HiCog className="w-8 h-8 text-light-green mr-3" />
                <h3 className="text-2xl font-serif font-semibold text-pure-white">Intelligent Pipeline Flow</h3>
              </div>
              
              <div className="space-y-4 mb-6 flex-grow">
                {['Bronze Layer', 'Silver Layer', 'Golden Layer', 'Diamond Layer'].map((layer, index) => (
                  <motion.div
                    key={layer}
                    className="flex items-center justify-between p-3 bg-background-tertiary rounded-lg"
                    initial={{ opacity: 0, x: -20 }}
                    whileInView={{ opacity: 1, x: 0 }}
                    transition={{ delay: index * 0.2 }}
                  >
                    <span className="text-crystal-white">{layer}</span>
                    <motion.div
                      className="w-8 h-8 rounded-full bg-light-green flex items-center justify-center"
                      animate={{ scale: [1, 1.2, 1] }}
                      transition={{ duration: 2, repeat: Infinity, delay: index * 0.5 }}
                    >
                      <span className="text-dark-bg font-bold text-sm">{index + 1}</span>
                    </motion.div>
                  </motion.div>
                ))}
              </div>
              
              <Link to="/login">
                <motion.button
                  className="w-full py-3 bg-light-green text-dark-bg font-semibold rounded-lg hover:bg-primary-600 transition-colors duration-300"
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <HiPlay className="inline w-5 h-5 mr-2" />
                  Run Pipeline Demo
                </motion.button>
              </Link>
            </motion.div>

            {/* Data Analytics Demo */}
            <motion.div
              className="bg-background-secondary rounded-lg border border-light-silver p-8 glass flex flex-col h-full"
              variants={cardVariants}
              whileHover={{ scale: 1.02 }}
            >
              <div className="flex items-center mb-6">
                <HiChartBar className="w-8 h-8 text-light-green mr-3" />
                <h3 className="text-2xl font-serif font-semibold text-pure-white">Intelligent Analytics</h3>
              </div>
              
              <div className="mb-6 flex-grow">
                {/* Simulated Chart */}
                <div className="h-48 bg-background-tertiary rounded-lg flex items-end justify-center p-4">
                  {[...Array(8)].map((_, i) => (
                    <motion.div
                      key={i}
                      className="bg-light-green rounded-t-lg mx-1"
                      style={{ width: '12px' }}
                      initial={{ height: 0 }}
                      whileInView={{ height: `${Math.random() * 120 + 30}px` }}
                      transition={{ delay: i * 0.1, duration: 0.6 }}
                    />
                  ))}
                </div>
                <p className="text-sm text-crystal-white mt-2 text-center">AI-Powered Market Insights</p>
                
                {/* Additional content to match height */}
                <div className="mt-4 space-y-3">
                  <div className="flex justify-between items-center text-sm text-crystal-white">
                    <span>AI Confidence:</span>
                    <span className="text-light-green font-semibold">87%</span>
                  </div>
                  <div className="flex justify-between items-center text-sm text-crystal-white">
                    <span>Real-time Updates:</span>
                    <span className="text-light-green font-semibold">Active</span>
                  </div>
                  <div className="flex justify-between items-center text-sm text-crystal-white">
                    <span>Data Sources:</span>
                    <span className="text-light-green font-semibold">BVMT Live</span>
                  </div>
                </div>
              </div>
              
              <Link to="/login">
                <motion.button
                  className="w-full py-3 bg-light-green text-dark-bg font-semibold rounded-lg hover:bg-primary-600 transition-colors duration-300"
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <HiTrendingUp className="inline w-5 h-5 mr-2" />
                  View Intelligence Demo
                </motion.button>
              </Link>
            </motion.div>
          </motion.div>
        </div>
      </section>

      {/* Tech Stack Section */}
      <section id="tech" className="py-20 bg-background-secondary">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div 
            className="text-center mb-16"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <motion.h2 
              className="text-4xl md:text-5xl font-serif font-bold text-pure-white mb-6"
              variants={itemVariants}
            >
              Technology Stack
            </motion.h2>
            <motion.p 
              className="text-xl text-crystal-white max-w-3xl mx-auto"
              variants={itemVariants}
            >
              Built with cutting-edge technologies for performance, scalability, and reliability
            </motion.p>
            
            <motion.div 
              className="flex justify-center my-8"
              variants={itemVariants}
            >
              <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent" />
            </motion.div>
          </motion.div>

          <motion.div 
            className="grid grid-cols-2 md:grid-cols-4 gap-6"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            {techStack.map((tech, index) => (
              <motion.div
                key={index}
                className="bg-background-tertiary rounded-lg border border-light-silver p-6 text-center glass"
                variants={cardVariants}
                whileHover={{ 
                  scale: 1.05, 
                  boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)",
                  y: -10
                }}
                transition={{ duration: 0.3 }}
              >
                <motion.div
                  className="w-16 h-16 rounded-lg flex items-center justify-center mx-auto mb-4 bg-white/10 backdrop-blur-sm"
                  whileHover={{ rotate: 360, scale: 1.1 }}
                  transition={{ duration: 0.6 }}
                >
                  <img 
                    src={tech.logo} 
                    alt={`${tech.name} logo`} 
                    className="w-10 h-10 object-contain"
                  />
                </motion.div>
                
                <h3 className="text-lg font-serif font-semibold text-pure-white mb-2">{tech.name}</h3>
                <p className="text-sm text-crystal-white">{tech.category}</p>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* Highlights Section */}
      <section id="highlights" className="py-20 bg-background-primary">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div 
            className="text-center mb-16"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <motion.h2 
              className="text-4xl md:text-5xl font-serif font-bold text-pure-white mb-6"
              variants={itemVariants}
            >
              Platform Highlights
            </motion.h2>
            <motion.p 
              className="text-xl text-crystal-white max-w-3xl mx-auto"
              variants={itemVariants}
            >
              Enterprise-grade performance, security, and reliability metrics
            </motion.p>
          </motion.div>

          <motion.div 
            className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8"
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            {highlights.map((highlight, index) => {
              const Icon = highlight.icon;
              return (
                <motion.div
                  key={index}
                  className="bg-background-secondary rounded-lg border border-light-silver p-8 text-center glass"
                  variants={cardVariants}
                  whileHover={{ 
                    scale: 1.05, 
                    boxShadow: "0 20px 40px rgba(144, 238, 144, 0.15)" 
                  }}
                >
                  <motion.div
                    className="w-16 h-16 bg-light-green rounded-lg flex items-center justify-center mx-auto mb-6"
                    whileHover={{ rotate: 360 }}
                    transition={{ duration: 0.6 }}
                  >
                    <Icon className="w-8 h-8 text-dark-bg" />
                  </motion.div>
                  
                  <motion.h3 
                    className="text-3xl font-display font-bold text-light-green mb-2"
                    initial={{ opacity: 0, scale: 0 }}
                    whileInView={{ opacity: 1, scale: 1 }}
                    transition={{ delay: index * 0.1 + 0.3 }}
                  >
                    {highlight.value}
                  </motion.h3>
                  
                  <h4 className="text-lg font-serif font-semibold text-pure-white mb-3">{highlight.title}</h4>
                  <p className="text-crystal-white text-sm">{highlight.description}</p>
                </motion.div>
              );
            })}
          </motion.div>
        </div>
      </section>

      {/* Get Started Section */}
      <section id="start" className="py-20 bg-background-secondary">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <motion.div 
            variants={containerVariants}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true }}
          >
            <motion.h2 
              className="text-4xl md:text-5xl font-serif font-bold text-pure-white mb-6"
              variants={itemVariants}
            >
              Ready to Get Started?
            </motion.h2>
            <motion.p 
              className="text-xl text-crystal-white mb-12 max-w-2xl mx-auto"
              variants={itemVariants}
            >
              Join the future of intelligent financial analytics. Start your journey with BVMT today.
            </motion.p>

            <motion.div 
              className="flex flex-col sm:flex-row gap-6 justify-center mb-12"
              variants={itemVariants}
            >
               <Link to="/register">
                 <motion.button
                   className="px-12 py-4 bg-light-green text-dark-bg font-display font-bold rounded-lg text-xl hover:bg-primary-600 transition-all duration-300 min-w-[200px]"
                   whileHover={{ scale: 1.05, boxShadow: "0 20px 40px rgba(144, 238, 144, 0.3)" }}
                   whileTap={{ scale: 0.95 }}
                 >
                   <HiArrowRight className="inline w-6 h-6 mr-3" />
                   Sign Up
                 </motion.button>
               </Link>
              
              <Link to="/register">
                <motion.button
                  className="px-12 py-4 border-2 border-light-green text-light-green font-display font-bold rounded-lg text-xl hover:bg-light-green hover:text-dark-bg transition-all duration-300 min-w-[200px]"
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                >
                  <HiUsers className="inline w-6 h-6 mr-3" />
                  Register
                </motion.button>
              </Link>
            </motion.div>

            {/* Contact Info */}
            <motion.div 
              className="bg-background-tertiary rounded-lg border border-light-silver p-8 glass"
              variants={cardVariants}
              whileHover={{ scale: 1.02 }}
            >
              <h3 className="text-xl font-serif font-semibold text-pure-white mb-4">Need Help Getting Started?</h3>
              <p className="text-crystal-white mb-6">
                Our intelligent analytics platform offers comprehensive documentation and support for all your financial intelligence needs.
              </p>
              
              <div className="flex flex-wrap gap-4 justify-center">
                <motion.button
                  className="px-6 py-2 bg-background-primary text-crystal-white rounded-lg hover:text-light-green transition-colors duration-300"
                  whileHover={{ scale: 1.05 }}
                >
                  📚 Documentation
                </motion.button>
                <motion.button
                  className="px-6 py-2 bg-background-primary text-crystal-white rounded-lg hover:text-light-green transition-colors duration-300"
                  whileHover={{ scale: 1.05 }}
                >
                  💬 Support Chat
                </motion.button>
                <motion.button
                  className="px-6 py-2 bg-background-primary text-crystal-white rounded-lg hover:text-light-green transition-colors duration-300"
                  whileHover={{ scale: 1.05 }}
                >
                  🎥 Video Tutorials
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        </div>
      </section>

      {/* Footer */}
      <footer className="bg-background-primary border-t border-light-silver py-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row justify-between items-center">
            <div className="flex items-center mb-4 md:mb-0">
              <motion.div
                className="w-10 h-10 bg-gradient-to-br from-background-tertiary to-background-secondary rounded-lg flex items-center justify-center mr-3 border border-light-green border-opacity-30"
                whileHover={{ scale: 1.1, rotate: 5 }}
                transition={{ duration: 0.3 }}
              >
                <img
                  src={bvmtLogo}
                  alt="BVMT Logo"
                  className="w-6 h-6 object-contain"
                  onError={(e) => {
                    e.target.style.display = 'none';
                    e.target.nextSibling.style.display = 'flex';
                  }}
                />
                <div className="w-6 h-6 bg-light-green rounded items-center justify-center hidden">
                  <HiDatabase className="w-4 h-4 text-dark-bg" />
                </div>
              </motion.div>
              <span className="text-xl font-serif font-bold text-pure-white">BVMT Intelligent Analytics</span>
            </div>
            
            <p className="text-crystal-white text-center">
              © 2024 BVMT Intelligent Analytics. Built with ❤️ for intelligent financial insights.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
}
