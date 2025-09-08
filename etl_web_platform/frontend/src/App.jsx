import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Loader from './components/common/Loader';
import Layout from './components/common/Layout';
import Dashboard from './pages/Dashboard';
import ETLPipeline from './pages/ETLPipeline';
import ScrapingMonitor from './pages/ScrapingMonitor';
import PowerBIDashboard from './components/powerbi/PowerBIDashboard';
import Profile from './pages/Profile';
import Login from './pages/Login';
import Register from './pages/Register';
import Welcome from './pages/Welcome';
import AIChatBot from './components/ai/AIChatBot';


function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [authKey, setAuthKey] = useState(0); // Key to force re-render

  // Global function to trigger auth check
  const triggerAuthCheck = () => {
    setAuthKey(prev => prev + 1);
  };

  // Make it available globally
  window.triggerAuthCheck = triggerAuthCheck;

  useEffect(() => {
    // Check if user is logged in by verifying session with backend
    const checkAuth = async () => {
      try {
        console.log('🔍 Checking authentication status...');
        
        // Check if we have local auth data first
        const user = localStorage.getItem('user');
        const isAuth = localStorage.getItem('isAuthenticated');
        
        // If no local auth data, user is not authenticated
        if (!user || isAuth !== 'true') {
          console.log('❌ No local authentication data found');
          setIsAuthenticated(false);
          return;
        }
        
        // Verify with backend using token-based auth
        const token = localStorage.getItem('authToken');
        console.log('🌐 Making auth check request to backend...');
        console.log('🔑 Using auth token:', token ? 'Token present' : 'No token');
        
        const response = await fetch('http://127.0.0.1:5000/api/auth/check', {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
          }
        });
        
        console.log('🔍 Auth check response status:', response.status);
        console.log('🔍 Response headers:', Object.fromEntries(response.headers.entries()));
      
        if (response.ok) {
          const data = await response.json();
          console.log('📦 Auth check response data:', data);
          if (data.authenticated) {
            console.log('✅ Backend confirms user is authenticated');
            setIsAuthenticated(true);
          } else {
            console.log('❌ Backend says user is not authenticated');
            // Clear any stale authentication data
            localStorage.removeItem('user');
            localStorage.removeItem('authToken');
            localStorage.removeItem('isAuthenticated');
            setIsAuthenticated(false);
          }
        } else if (response.status === 401) {
          // 401 means session doesn't exist or expired
          console.log('⚠️ 401: Session not found - clearing local auth data');
          localStorage.removeItem('user');
          localStorage.removeItem('authToken');
          localStorage.removeItem('isAuthenticated');
          setIsAuthenticated(false);
        } else {
          // Other errors should be logged
          console.error('❌ Auth check failed with status:', response.status);
          localStorage.removeItem('user');
          localStorage.removeItem('authToken');
          localStorage.removeItem('isAuthenticated');
          setIsAuthenticated(false);
        }
    } catch (error) {
      console.error('Auth check error:', error);
      // Clear any stale authentication data
      localStorage.removeItem('user');
      localStorage.removeItem('authToken');
      localStorage.removeItem('isAuthenticated');
      setIsAuthenticated(false);
    }
    };

    // Run auth check and enforce minimum 3-second loader duration
    const startTime = Date.now();
    
    checkAuth().finally(() => {
      const elapsed = Date.now() - startTime;
      const remainingTime = Math.max(3000 - elapsed, 0);
      
      setTimeout(() => {
        setIsLoading(false);
      }, remainingTime);
    });
  }, [authKey]);

  if (isLoading) {
    return <Loader />;
  }

  return (
    <Router>
      <Routes>
        {/* Welcome page - accessible to everyone */}
        <Route path="/welcome" element={<Welcome />} />
        
        {/* Authentication routes */}
        <Route path="/login" element={
          isAuthenticated ? <Navigate to="/dashboard" replace /> : <Login />
        } />
        <Route path="/register" element={
          isAuthenticated ? <Navigate to="/dashboard" replace /> : <Register />
        } />
        
        {/* Protected routes */}
        <Route path="/dashboard" element={
          isAuthenticated ? (
            <Layout>
              <Dashboard />
              <AIChatBot />
            </Layout>
          ) : (
            <Navigate to="/welcome" replace />
          )
        } />
        <Route path="/etl-pipeline" element={
          isAuthenticated ? (
            <Layout>
              <ETLPipeline />
              <AIChatBot />
            </Layout>
          ) : (
            <Navigate to="/welcome" replace />
          )
        } />
        <Route path="/scraping-monitor" element={
          isAuthenticated ? (
            <Layout>
              <ScrapingMonitor />
              <AIChatBot />
            </Layout>
          ) : (
            <Navigate to="/welcome" replace />
          )
        } />
        <Route path="/powerbi-dashboard" element={
          isAuthenticated ? (
            <Layout>
              <PowerBIDashboard />
              <AIChatBot />
            </Layout>
          ) : (
            <Navigate to="/welcome" replace />
          )
        } />
        <Route path="/profile" element={
          isAuthenticated ? (
            <Layout>
              <Profile />
              <AIChatBot />
            </Layout>
          ) : (
            <Navigate to="/welcome" replace />
          )
        } />
        
        {/* Default route */}
        <Route path="/" element={
          isAuthenticated ? <Navigate to="/dashboard" replace /> : <Navigate to="/welcome" replace />
        } />
      </Routes>
    </Router>
  );
}

export default App;
