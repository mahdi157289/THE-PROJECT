import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { HiUser, HiMail, HiCalendar, HiPencil, HiCheck, HiX, HiLockClosed, HiEye, HiEyeOff, HiCheckCircle } from 'react-icons/hi';

const Profile = () => {
  const [user, setUser] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isEditing, setIsEditing] = useState(false);
  const [isChangingPassword, setIsChangingPassword] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [isSaving, setIsSaving] = useState(false);

  // Edit form data
  const [editData, setEditData] = useState({
    username: '',
    email: ''
  });

  // Password change data
  const [passwordData, setPasswordData] = useState({
    currentPassword: '',
    newPassword: '',
    confirmPassword: ''
  });

  useEffect(() => {
    // Debug localStorage contents
    console.log('🔍 [PROFILE] localStorage contents:');
    console.log('  - user:', localStorage.getItem('user'));
    console.log('  - authToken:', localStorage.getItem('authToken') ? `${localStorage.getItem('authToken').substring(0, 20)}...` : 'null');
    console.log('  - isAuthenticated:', localStorage.getItem('isAuthenticated'));
    
    const userData = localStorage.getItem('user');
    if (userData) {
      const parsedUser = JSON.parse(userData);
      setUser(parsedUser);
      setEditData({
        username: parsedUser.username || '',
        email: parsedUser.email || ''
      });
    }
    setIsLoading(false);
  }, []);

  // Clear messages after 5 seconds
  useEffect(() => {
    if (success || error) {
      const timer = setTimeout(() => {
        setSuccess('');
        setError('');
      }, 5000);
      return () => clearTimeout(timer);
    }
  }, [success, error]);

  const handleEdit = () => {
    setIsEditing(true);
    setIsChangingPassword(false);
    setError('');
    setSuccess('');
  };

  const handleCancel = () => {
    setIsEditing(false);
    setIsChangingPassword(false);
    setEditData({
      username: user?.username || '',
      email: user?.email || ''
    });
    setPasswordData({
      currentPassword: '',
      newPassword: '',
      confirmPassword: ''
    });
    setError('');
  };

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setEditData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handlePasswordChange = (e) => {
    const { name, value } = e.target;
    setPasswordData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSaveProfile = async () => {
    try {
      setError('');
      setIsSaving(true);
      
      // Validate input
      if (!editData.username.trim() || !editData.email.trim()) {
        setError('Username and email are required');
        return;
      }

      // Get auth token
      const token = localStorage.getItem('authToken');
      console.log('🔍 [PROFILE] Token from localStorage:', token ? `${token.substring(0, 20)}...` : 'null');
      
      if (!token) {
        console.error('❌ [PROFILE] No auth token found in localStorage');
        setError('Authentication required. Please log in again.');
        return;
      }

      console.log('🔍 [PROFILE] Sending PUT request to profile endpoint...');
      console.log('🔍 [PROFILE] Request body:', { username: editData.username, email: editData.email });

      // Send update request to backend
      const response = await fetch('http://127.0.0.1:5000/api/auth/profile', {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          username: editData.username,
          email: editData.email
        })
      });

      console.log('🔍 [PROFILE] Response status:', response.status);
      console.log('🔍 [PROFILE] Response headers:', Object.fromEntries(response.headers.entries()));

      const data = await response.json();

      if (response.ok) {
        // Update local storage and state
        const updatedUser = {
          ...user,
          username: editData.username,
          email: editData.email
        };
        
        localStorage.setItem('user', JSON.stringify(updatedUser));
        setUser(updatedUser);
        setIsEditing(false);
        setSuccess('Profile updated successfully!');
      } else {
        setError(data.message || 'Failed to update profile');
      }
    } catch (error) {
      setError('Network error. Please try again.');
    } finally {
      setIsSaving(false);
    }
  };

  const handleChangePassword = async () => {
    try {
      setError('');
      setIsSaving(true);
      
      // Validate passwords
      if (!passwordData.currentPassword || !passwordData.newPassword || !passwordData.confirmPassword) {
        setError('All password fields are required');
        return;
      }

      if (passwordData.newPassword !== passwordData.confirmPassword) {
        setError('New passwords do not match');
        return;
      }

      if (passwordData.newPassword.length < 6) {
        setError('New password must be at least 6 characters long');
        return;
      }

      // Get auth token
      const token = localStorage.getItem('authToken');
      if (!token) {
        setError('Authentication required. Please log in again.');
        return;
      }

      // Send password change request to backend
      const response = await fetch('http://127.0.0.1:5000/api/auth/change-password', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          currentPassword: passwordData.currentPassword,
          newPassword: passwordData.newPassword
        })
      });

      const data = await response.json();

      if (response.ok) {
        setIsChangingPassword(false);
        setPasswordData({
          currentPassword: '',
          newPassword: '',
          confirmPassword: ''
        });
        setSuccess('Password changed successfully!');
      } else {
        setError(data.message || 'Failed to change password');
      }
    } catch (error) {
      setError('Network error. Please try again.');
    } finally {
      setIsSaving(false);
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-green-600"></div>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-2xl font-serif text-white mb-4">No user data found</h2>
          <p className="text-gray-300">Please log in again</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-black p-6">
      <div className="max-w-4xl mx-auto">
        {/* Page Title */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-center mb-8"
        >
          <h1 className="text-4xl font-serif text-white mb-4">
            👤 User Profile
          </h1>
          <p className="text-lg text-gray-300">
            Manage your account information and security
          </p>
          {/* Long interrupted line */}
          <div className="w-96 h-0.5 bg-gradient-to-r from-transparent via-green-400 to-transparent opacity-60 mx-auto mt-6"></div>
        </motion.div>

        {/* Success Message */}
        <AnimatePresence>
          {success && (
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="mb-6 p-4 bg-green-100 border border-green-400 text-green-700 rounded-lg text-center glass"
            >
              <div className="flex items-center justify-center space-x-2">
                <HiCheck className="w-5 h-5" />
                <span>{success}</span>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Error Message */}
        <AnimatePresence>
          {error && (
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="mb-6 p-4 bg-red-100 border border-red-400 text-red-700 rounded-lg text-center glass"
            >
              <div className="flex items-center justify-center space-x-2">
                <HiX className="w-5 h-5" />
                <span>{error}</span>
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Profile Card */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="bg-white rounded-2xl shadow-2xl overflow-hidden glass"
          whileHover={{ boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)" }}
        >
          {/* Card Header */}
          <div className="bg-gradient-to-r from-green-600 to-emerald-600 px-8 py-6">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-4">
                <div className="w-16 h-16 bg-white rounded-full flex items-center justify-center">
                  <HiUser className="w-8 h-8 text-green-600" />
                </div>
                <div>
                  <h2 className="text-2xl font-serif text-white">{user.username}</h2>
                  <p className="text-green-100">{user.email}</p>
                </div>
              </div>
              
              {!isEditing && !isChangingPassword ? (
                <div className="flex space-x-2">
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={handleEdit}
                    className="bg-white text-green-600 px-4 py-2 rounded-lg flex items-center space-x-2 hover:bg-green-50 transition-colors font-medium"
                  >
                    <HiPencil className="w-4 h-4" />
                    <span>Edit Profile</span>
                  </motion.button>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={() => {
                      setIsChangingPassword(true);
                      setIsEditing(false);
                      setError('');
                      setSuccess('');
                    }}
                    className="bg-white text-green-600 px-4 py-2 rounded-lg flex items-center space-x-2 hover:bg-green-50 transition-colors font-medium"
                  >
                    <HiLockClosed className="w-4 h-4" />
                    <span>Change Password</span>
                  </motion.button>
                </div>
              ) : (
                <div className="flex space-x-2">
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={isEditing ? handleSaveProfile : handleChangePassword}
                    disabled={isSaving}
                    className="bg-green-500 text-white px-4 py-2 rounded-lg flex items-center space-x-2 hover:bg-green-600 transition-colors font-medium disabled:opacity-50"
                  >
                    {isSaving ? (
                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    ) : (
                      <HiCheck className="w-4 h-4" />
                    )}
                    <span>{isSaving ? 'Saving...' : 'Save'}</span>
                  </motion.button>
                  <motion.button
                    whileHover={{ scale: 1.05 }}
                    whileTap={{ scale: 0.95 }}
                    onClick={handleCancel}
                    disabled={isSaving}
                    className="bg-gray-500 text-white px-4 py-2 rounded-lg flex items-center space-x-2 hover:bg-gray-600 transition-colors font-medium disabled:opacity-50"
                  >
                    <HiX className="w-4 h-4" />
                    <span>Cancel</span>
                  </motion.button>
                </div>
              )}
            </div>
          </div>

          {/* Card Content */}
          <div className="p-8">
            <AnimatePresence mode="wait">
              {isEditing ? (
                <motion.div
                  key="editing"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  className="space-y-6"
                >
                  <h3 className="text-xl font-serif text-gray-800 mb-6 text-center">
                    Edit Profile Information
                  </h3>
                  
                  <div className="grid md:grid-cols-2 gap-6">
                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiUser className="w-5 h-5 text-green-600" />
                        <span>Username</span>
                      </label>
                      <input
                        type="text"
                        name="username"
                        value={editData.username}
                        onChange={handleInputChange}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all text-gray-800 font-medium bg-white"
                        placeholder="Enter username"
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiMail className="w-5 h-5 text-green-600" />
                        <span>Email</span>
                      </label>
                      <input
                        type="email"
                        name="email"
                        value={editData.email}
                        onChange={handleInputChange}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all text-gray-800 font-medium bg-white"
                        placeholder="Enter email"
                      />
                    </div>
                  </div>
                </motion.div>
              ) : isChangingPassword ? (
                <motion.div
                  key="password"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  className="space-y-6"
                >
                  <h3 className="text-xl font-serif text-gray-800 mb-6 text-center">
                    Change Password
                  </h3>
                  
                  <div className="space-y-4 max-w-md mx-auto">
                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiLockClosed className="w-5 h-5 text-green-600" />
                        <span>Current Password</span>
                      </label>
                      <div className="relative">
                        <input
                          type={showPassword ? "text" : "password"}
                          name="currentPassword"
                          value={passwordData.currentPassword}
                          onChange={handlePasswordChange}
                          className="w-full px-4 py-3 pr-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all text-gray-800 font-medium bg-white"
                          placeholder="Enter current password"
                        />
                        <button
                          type="button"
                          onClick={() => setShowPassword(!showPassword)}
                          className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-gray-600"
                        >
                          {showPassword ? <HiEyeOff className="w-5 h-5" /> : <HiEye className="w-5 h-5" />}
                        </button>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiLockClosed className="w-5 h-5 text-green-600" />
                        <span>New Password</span>
                      </label>
                      <input
                        type={showPassword ? "text" : "password"}
                        name="newPassword"
                        value={passwordData.newPassword}
                        onChange={handlePasswordChange}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all text-gray-800 font-medium bg-white"
                        placeholder="Enter new password"
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiLockClosed className="w-5 h-5 text-green-600" />
                        <span>Confirm New Password</span>
                      </label>
                      <input
                        type={showPassword ? "text" : "password"}
                        name="confirmPassword"
                        value={passwordData.confirmPassword}
                        onChange={handlePasswordChange}
                        className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-green-500 focus:border-transparent transition-all text-gray-800 font-medium bg-white"
                        placeholder="Confirm new password"
                      />
                    </div>
                  </div>
                </motion.div>
              ) : (
                <motion.div
                  key="viewing"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  className="grid md:grid-cols-2 gap-8"
                >
                  <div className="space-y-6">
                    <h3 className="text-xl font-serif text-gray-800 mb-4 text-center">
                      Account Information
                    </h3>
                    
                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiUser className="w-5 h-5 text-green-600" />
                        <span>Username</span>
                      </label>
                      <div className="bg-gray-50 px-4 py-3 rounded-lg text-gray-800 font-medium">
                        {user.username}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiMail className="w-5 h-5 text-green-600" />
                        <span>Email</span>
                      </label>
                      <div className="bg-gray-50 px-4 py-3 rounded-lg text-gray-800 font-medium">
                        {user.email}
                      </div>
                    </div>
                  </div>

                  <div className="space-y-6">
                    <h3 className="text-xl font-serif text-gray-800 mb-4 text-center">
                      Account Details
                    </h3>
                    
                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiCalendar className="w-5 h-5 text-green-600" />
                        <span>User ID</span>
                      </label>
                      <div className="bg-gray-50 px-4 py-3 rounded-lg text-gray-800 font-medium">
                        #{user.id || 'N/A'}
                      </div>
                    </div>

                    <div className="space-y-2">
                      <label className="flex items-center space-x-2 text-gray-700 font-medium bg-gray-100 px-3 py-2 rounded-lg">
                        <HiCheckCircle className="w-5 h-5 text-green-600" />
                        <span>Account Status</span>
                      </label>
                      <div className="bg-green-50 px-4 py-3 rounded-lg text-green-700 font-medium">
                        Active
                      </div>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </motion.div>

        {/* Interrupted line */}
        <div className="w-48 h-0.5 bg-gradient-to-r from-transparent via-green-400 to-transparent opacity-60 mx-auto my-12"></div>

        {/* Additional Info Card */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.4 }}
          className="bg-white rounded-2xl shadow-2xl p-8 glass"
          whileHover={{ boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)" }}
        >
          <h3 className="text-xl font-serif text-gray-800 mb-6 text-center">
            Security & Privacy
          </h3>
          
          <div className="text-center text-gray-600 space-y-2">
            <p>Your account is secured with token-based authentication.</p>
            <p>Last login: {new Date().toLocaleDateString()}</p>
            <p>Your data is protected and encrypted.</p>
          </div>
        </motion.div>
      </div>
    </div>
  );
};

export default Profile;