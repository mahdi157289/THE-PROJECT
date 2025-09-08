import React, { useState, useEffect } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  HiHome, 
  HiDatabase, 
  HiCog, 
  HiChartBar, 
  HiChartPie, 
  HiMenu, 
  HiX, 
  HiChevronLeft, 
  HiChevronRight,
  HiLogout,
  HiUser
} from 'react-icons/hi';
import { BourseLogo } from './BourseLogo';

const navigation = [
  { name: 'Dashboard', href: '/dashboard', icon: HiHome },
  { name: 'ETL Pipeline', href: '/etl-pipeline', icon: HiDatabase },
  { name: 'Scraping Monitor', href: '/scraping-monitor', icon: HiCog },
  { name: 'Power BI Dashboard', href: '/powerbi-dashboard', icon: HiChartBar },
  { name: 'Profile', href: '/profile', icon: HiUser },
];

const sidebarVariants = {
  open: { width: "16rem", transition: { type: "spring", stiffness: 300, damping: 30 } },
  closed: { width: "4rem", transition: { type: "spring", stiffness: 300, damping: 30 } }
};

const menuItemVariants = {
  closed: { opacity: 0, x: -20 },
  open: { opacity: 1, x: 0 }
};

const staggerContainer = {
  open: {
    transition: {
      staggerChildren: 0.1
    }
  }
};

export default function Layout({ children }) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [user, setUser] = useState(null);
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    // Check if user is logged in
    const storedUser = localStorage.getItem('user');
    if (storedUser) {
      setUser(JSON.parse(storedUser));
    }
  }, []);

  const handleLogout = async () => {
    try {
      const token = localStorage.getItem('authToken');
      
      const response = await fetch('http://127.0.0.1:5000/api/auth/logout', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        }
      });

      // Clear frontend state regardless of backend response
      localStorage.removeItem('user');
      localStorage.removeItem('authToken');
      localStorage.removeItem('isAuthenticated');
      setUser(null);
      
      console.log('✅ Logout successful - token invalidated');
      
      // Trigger App.jsx auth check to update routing
      if (window.triggerAuthCheck) {
        console.log('🔄 Triggering auth check after logout...');
        window.triggerAuthCheck();
      }
      
      // Navigate to welcome
      navigate('/welcome');
      
    } catch (error) {
      console.error('Logout error:', error);
      // Still clear frontend state
      localStorage.removeItem('user');
      localStorage.removeItem('authToken');
      localStorage.removeItem('isAuthenticated');
      setUser(null);
      
      // Trigger App.jsx auth check
      if (window.triggerAuthCheck) {
        window.triggerAuthCheck();
      }
      
      navigate('/welcome');
    }
  };

  const toggleSidebar = () => {
    setSidebarCollapsed(!sidebarCollapsed);
  };

  return (
    <div className="min-h-screen bg-dark-bg font-sans">
      {/* Sidebar */}
      <motion.div 
        className={`fixed inset-y-0 left-0 z-50 bg-background-secondary shadow-xl border-r border-light-silver overflow-hidden`}
        variants={sidebarVariants}
        animate={sidebarCollapsed ? "closed" : "open"}
        initial="open"
      >
        <motion.div 
          className={`flex items-center h-16 border-b border-light-silver ${sidebarCollapsed ? 'justify-center px-2' : 'justify-between px-4'}`}
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
        >
          <div className="flex items-center">
            <motion.div
              whileHover={{ scale: 1.1 }}
              whileTap={{ scale: 0.95 }}
            >
              <BourseLogo size="w-8 h-8" />
            </motion.div>
            <AnimatePresence>
              {!sidebarCollapsed && (
                <motion.span 
                  className="ml-3 text-lg font-display font-semibold text-pure-white"
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  transition={{ duration: 0.3 }}
                >
                  Bourse de Tunis
                </motion.span>
              )}
            </AnimatePresence>
          </div>
          {!sidebarCollapsed && (
            <motion.button
              onClick={toggleSidebar}
              className="p-2 rounded-md text-crystal-white hover:text-light-green hover:bg-background-tertiary transition-all duration-300"
              whileHover={{ scale: 1.1 }}
              whileTap={{ scale: 0.95 }}
            >
              <HiChevronLeft className="h-5 w-5" />
            </motion.button>
          )}
        </motion.div>
        
        <motion.nav 
          className="mt-5 px-2"
          variants={staggerContainer}
          initial="closed"
          animate="open"
        >
          {/* Expand button when collapsed */}
          {sidebarCollapsed && (
            <motion.button
              onClick={toggleSidebar}
              className="w-full mb-4 p-2 rounded-md text-crystal-white hover:text-light-green hover:bg-background-tertiary transition-all duration-300 flex justify-center"
              whileHover={{ scale: 1.1 }}
              whileTap={{ scale: 0.95 }}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.3 }}
            >
              <HiChevronRight className="h-5 w-5" />
            </motion.button>
          )}
          
          <div className="space-y-1">
            {navigation.map((item, index) => {
              const isActive = location.pathname === item.href;
              return (
                <motion.div
                  key={item.name}
                  variants={menuItemVariants}
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                >
                  <Link
                    to={item.href}
                    className={`group flex items-center ${sidebarCollapsed ? 'justify-center px-3 py-3' : 'px-4 py-3'} text-sm font-medium rounded-lg transition-all duration-300 ${
                      isActive
                        ? 'bg-light-green text-dark-bg shadow-lg'
                        : 'text-crystal-white hover:bg-background-tertiary hover:text-light-green'
                    }`}
                    title={sidebarCollapsed ? item.name : ''}
                  >
                    <motion.div
                      whileHover={{ rotate: 5 }}
                      transition={{ duration: 0.2 }}
                    >
                      <item.icon className={`h-5 w-5 flex-shrink-0 ${sidebarCollapsed ? '' : 'mr-3'}`} />
                    </motion.div>
                    <AnimatePresence>
                      {!sidebarCollapsed && (
                        <motion.span
                          initial={{ opacity: 0, x: -10 }}
                          animate={{ opacity: 1, x: 0 }}
                          exit={{ opacity: 0, x: -10 }}
                          transition={{ duration: 0.2 }}
                        >
                          {item.name}
                        </motion.span>
                      )}
                    </AnimatePresence>
                  </Link>
                </motion.div>
              );
            })}
          </div>
          
          {/* Logout Button */}
          {user && (
            <motion.div 
              className="mt-auto px-2 pb-4"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.5 }}
            >
              <motion.button
                onClick={handleLogout}
                className={`w-full flex items-center ${sidebarCollapsed ? 'justify-center px-3 py-2' : 'px-3 py-2'} text-sm font-medium text-crystal-white hover:bg-background-tertiary hover:text-light-green rounded-md transition-all duration-300`}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
                title={sidebarCollapsed ? 'Logout' : ''}
              >
                <HiLogout className={`h-5 w-5 ${sidebarCollapsed ? '' : 'mr-3'}`} />
                <AnimatePresence>
                  {!sidebarCollapsed && (
                    <motion.span
                      initial={{ opacity: 0, x: -10 }}
                      animate={{ opacity: 1, x: 0 }}
                      exit={{ opacity: 0, x: -10 }}
                      transition={{ duration: 0.2 }}
                    >
                      Logout
                    </motion.span>
                  )}
                </AnimatePresence>
              </motion.button>
            </motion.div>
          )}
        </motion.nav>
      </motion.div>

      {/* Main content */}
      <motion.div 
        className={`transition-all duration-300 ease-in-out ${sidebarCollapsed ? 'ml-16' : 'ml-64'}`}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5 }}
      >
        {/* Mobile header */}
        <motion.div 
          className="lg:hidden bg-background-secondary shadow-lg border-b border-light-silver"
          initial={{ y: -50, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          transition={{ duration: 0.5 }}
        >
          <div className="flex items-center justify-between px-4 py-3">
            <div className="flex items-center">
              <motion.div
                whileHover={{ scale: 1.1 }}
                whileTap={{ scale: 0.95 }}
              >
                <BourseLogo size="w-8 h-8" />
              </motion.div>
              <span className="ml-3 text-lg font-display font-semibold text-pure-white">Bourse de Tunis</span>
            </div>
            <motion.button
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
              className="p-2 rounded-md text-crystal-white hover:text-light-green hover:bg-background-tertiary transition-all duration-300"
              whileHover={{ scale: 1.1 }}
              whileTap={{ scale: 0.95 }}
            >
              {mobileMenuOpen ? <HiX className="h-6 w-6" /> : <HiMenu className="h-6 w-6" />}
            </motion.button>
          </div>
        </motion.div>

        {/* Mobile menu */}
        <AnimatePresence>
          {mobileMenuOpen && (
            <motion.div 
              className="lg:hidden bg-background-secondary border-b border-light-silver"
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: "auto", opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.3 }}
            >
              <nav className="px-2 pt-2 pb-3 space-y-1">
                {navigation.map((item, index) => {
                  const isActive = location.pathname === item.href;
                  return (
                    <motion.div
                      key={item.name}
                      initial={{ opacity: 0, x: -20 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: index * 0.1 }}
                      whileHover={{ scale: 1.02 }}
                      whileTap={{ scale: 0.98 }}
                    >
                      <Link
                        to={item.href}
                        className={`block px-3 py-2 rounded-md text-base font-medium transition-all duration-300 ${
                          isActive
                            ? 'bg-light-green text-dark-bg'
                            : 'text-crystal-white hover:bg-background-tertiary hover:text-light-green'
                        }`}
                        onClick={() => setMobileMenuOpen(false)}
                      >
                        {item.name}
                      </Link>
                    </motion.div>
                  );
                })}
              </nav>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Page content */}
        <motion.main 
          className="p-6 bg-dark-bg"
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5, delay: 0.2 }}
        >
          {children}
        </motion.main>
      </motion.div>
    </div>
  );
}
