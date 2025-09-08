import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Link, useNavigate } from 'react-router-dom';
import { HiEye, HiEyeOff, HiLockClosed, HiUser, HiMail, HiCheck } from 'react-icons/hi';

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

export default function Register() {
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    password: '',
    confirmPassword: ''
  });
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const navigate = useNavigate();

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsLoading(true);
    setError('');
    setSuccess('');

    // Validation
    if (formData.password !== formData.confirmPassword) {
      setError('Passwords do not match');
      setIsLoading(false);
      return;
    }

    if (formData.password.length < 6) {
      setError('Password must be at least 6 characters long');
      setIsLoading(false);
      return;
    }

    try {
      const response = await fetch('http://127.0.0.1:5000/api/auth/register', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username: formData.username,
          email: formData.email,
          password: formData.password
        }),
      });

      const data = await response.json();

      if (response.ok) {
        setSuccess('Account created successfully! Redirecting to login...');
        setTimeout(() => {
          navigate('/login');
        }, 2000);
      } else {
        setError(data.message || 'Registration failed');
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
            <HiUser className="w-8 h-8 text-dark-bg" />
          </motion.div>
          <h1 className="text-3xl font-serif font-bold text-pure-white mb-2 text-center">Create Account</h1>
          <p className="text-crystal-white font-sans text-center">Join the BVMT ETL Platform</p>
          
          {/* Long Interrupted Line */}
          <motion.div 
            className="flex justify-center my-6"
            variants={itemVariants}
          >
            <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-light-green to-transparent opacity-60"></div>
          </motion.div>
        </motion.div>

        {/* Register Form */}
        <motion.div 
          className="bg-background-secondary rounded-lg shadow-xl border border-light-silver p-8 glass"
          variants={itemVariants}
          whileHover={{ scale: 1.02 }}
        >
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Username Field */}
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
                  placeholder="Choose a username"
                />
              </div>
            </motion.div>

            {/* Email Field */}
            <motion.div variants={itemVariants}>
              <label className="block text-sm font-display font-medium text-crystal-white mb-2">
                Email Address
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <HiMail className="h-5 w-5 text-pearl-white" />
                </div>
                <input
                  type="email"
                  name="email"
                  value={formData.email}
                  onChange={handleChange}
                  required
                  className="w-full pl-10 pr-3 py-3 bg-background-tertiary border border-light-silver rounded-lg focus:outline-none focus:ring-2 focus:ring-light-green focus:border-transparent text-pure-white placeholder-pearl-white font-sans"
                  placeholder="Enter your email"
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
                  placeholder="Create a password"
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

            {/* Confirm Password Field */}
            <motion.div variants={itemVariants}>
              <label className="block text-sm font-display font-medium text-crystal-white mb-2">
                Confirm Password
              </label>
              <div className="relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                  <HiLockClosed className="h-5 w-5 text-pearl-white" />
                </div>
                <input
                  type={showConfirmPassword ? 'text' : 'password'}
                  name="confirmPassword"
                  value={formData.confirmPassword}
                  onChange={handleChange}
                  required
                  className="w-full pl-10 pr-12 py-3 bg-background-tertiary border border-light-silver rounded-lg focus:outline-none focus:ring-2 focus:ring-light-green focus:border-transparent text-pure-white placeholder-pearl-white font-sans"
                  placeholder="Confirm your password"
                />
                <motion.button
                  type="button"
                  className="absolute inset-y-0 right-0 pr-3 flex items-center"
                  onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                >
                  {showConfirmPassword ? (
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

            {/* Success Message */}
            {success && (
              <motion.div 
                className="p-3 bg-light-green bg-opacity-20 border border-light-green rounded-lg"
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
              >
                <div className="flex items-center">
                  <HiCheck className="h-5 w-5 text-light-green mr-2" />
                  <p className="text-light-green text-sm font-sans">{success}</p>
                </div>
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
                'Create Account'
              )}
            </motion.button>
          </form>

          {/* Login Link */}
          <motion.div 
            className="mt-6 text-center"
            variants={itemVariants}
          >
            <p className="text-crystal-white font-sans">
              Already have an account?{' '}
              <Link 
                to="/login" 
                className="text-light-green hover:text-primary-600 font-display font-medium transition-colors duration-200"
              >
                Sign in here
              </Link>
            </p>
          </motion.div>
        </motion.div>
      </motion.div>
    </div>
  );
}

