import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Link, useNavigate } from 'react-router-dom';
import { HiEye, HiEyeOff, HiLockClosed, HiUser } from 'react-icons/hi';

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

export default function Login() {
  const [formData, setFormData] = useState({
    username: '',
    password: ''
  });
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    setError('');

    try {
      const response = await fetch('http://127.0.0.1:5000/api/auth/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include', // Include cookies for session management
        body: JSON.stringify(formData),
      });

      const data = await response.json();
      console.log('🔐 Login response:', data);
      console.log('🔐 Login response headers:', Object.fromEntries(response.headers.entries()));
      console.log('🍪 Cookies after login response:', document.cookie);

      if (response.ok) {
        // Store user info, token, and set authentication flag
        localStorage.setItem('user', JSON.stringify(data.user));
        localStorage.setItem('authToken', data.token);
        localStorage.setItem('isAuthenticated', 'true');
        
        console.log('✅ Login successful, user stored:', data.user);
        console.log('🔑 Auth token stored:', data.token);
        
        // Trigger auth check immediately (no delay needed for tokens)
        if (window.triggerAuthCheck) {
          console.log('🔄 Triggering auth check after login...');
          window.triggerAuthCheck();
        }
        
        // Navigate to dashboard
        console.log('🚀 Navigating to dashboard...');
        navigate('/');
      } else {
        setError(data.message || 'Login failed');
      }
    } catch (error) {
      setError('Connection error. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  const handleChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
  };

  return (
    <div className="min-h-screen bg-dark-bg flex items-center justify-center p-4">
      <motion.div 
        className="w-full max-w-md"
        variants={containerVariants}
        initial="hidden"
        animate="visible"
      >
        {/* Logo and Title */}
        <motion.div 
          className="text-center mb-8"
          variants={itemVariants}
        >
          <motion.div
            className="w-16 h-16 bg-light-green rounded-full flex items-center justify-center mx-auto mb-4"
            whileHover={{ scale: 1.1, rotate: 360 }}
            transition={{ duration: 0.6 }}
          >
            <HiLockClosed className="w-8 h-8 text-dark-bg" />
          </motion.div>
          <h1 className="text-3xl font-serif font-bold text-pure-white mb-2 text-center">Welcome Back</h1>
          <p className="text-crystal-white font-sans text-center">Sign in to your BVMT ETL Platform account</p>
          
          {/* Long Interrupted Line */}
          <motion.div 
            className="flex justify-center my-6"
            variants={itemVariants}
          >
            <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
          </motion.div>
        </motion.div>

        {/* Login Form */}
        <motion.div 
          className="bg-background-secondary rounded-lg shadow-xl border border-light-silver p-8 glass"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Email Field */}
            <motion.div variants={itemVariants}>
              <label className="block text-sm font-display font-medium text-crystal-white mb-2">
                Username
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <HiUser className="h-5 w-5 text-pearl-white" />
                </div>
                <input
                  type="text"
                  name="username"
                  value={formData.username}
                  onChange={handleChange}
                  required
                  className="w-full pl-10 pr-3 py-3 bg-background-tertiary border border-light-silver rounded-lg focus:outline-none focus:ring-2 focus:ring-light-green focus:border-transparent text-pure-white placeholder-pearl-white font-sans"
                  placeholder="Enter your username"
                />
              </div>
            </motion.div>

            {/* Password Field */}
            <motion.div variants={itemVariants}>
              <label className="block text-sm font-display font-medium text-crystal-white mb-2">
                Password
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <HiLockClosed className="h-5 w-5 text-pearl-white" />
                </div>
                <input
                  type={showPassword ? 'text' : 'password'}
                  name="password"
                  value={formData.password}
                  onChange={handleChange}
                  required
                  className="w-full pl-10 pr-12 py-3 bg-background-tertiary border border-light-silver rounded-lg focus:outline-none focus:ring-2 focus:ring-light-green focus:border-transparent text-pure-white placeholder-pearl-white font-sans"
                  placeholder="Enter your password"
                />
                <motion.button
                  type="button"
                  className="absolute inset-y-0 right-0 pr-3 flex items-center"
                  onClick={() => setShowPassword(!showPassword)}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                >
                  {showPassword ? (
                    <HiEyeOff className="h-5 w-5 text-pearl-white" />
                  ) : (
                    <HiEye className="h-5 w-5 text-pearl-white" />
                  )}
                </motion.button>
              </div>
            </motion.div>

            {/* Error Message */}
            {error && (
              <motion.div 
                className="p-3 bg-red-400 bg-opacity-20 border border-red-400 rounded-lg"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
              >
                <p className="text-red-400 text-sm font-sans">{error}</p>
              </motion.div>
            )}

            {/* Submit Button */}
            <motion.button
              type="submit"
              disabled={isLoading}
              className="w-full bg-light-green hover:bg-primary-600 disabled:bg-pearl-white disabled:opacity-50 text-dark-bg font-display font-semibold py-3 px-4 rounded-lg transition-all duration-300 disabled:cursor-not-allowed"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              {isLoading ? (
                <motion.div
                  className="flex items-center justify-center"
                  animate={{ rotate: 360 }}
                  transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                >
                  <div className="w-5 h-5 border-2 border-dark-bg border-t-transparent rounded-full"></div>
                </motion.div>
              ) : (
                'Sign In'
              )}
            </motion.button>
          </form>

          {/* Register Link */}
          <motion.div 
            className="mt-6 text-center"
            variants={itemVariants}
          >
            <p className="text-crystal-white font-sans">
              Don't have an account?{' '}
              <Link 
                to="/register" 
                className="text-light-green hover:text-primary-600 font-display font-medium transition-colors duration-200"
              >
                Sign up here
              </Link>
            </p>
          </motion.div>
        </motion.div>
      </motion.div>
    </div>
  );
}
