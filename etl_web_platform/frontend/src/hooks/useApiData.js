import { useState, useEffect, useCallback } from 'react';

/**
 * Custom hook for API data fetching with loading states and error handling
 * @param {Function} apiFunction - API function to call
 * @param {number} refreshInterval - Auto-refresh interval in milliseconds (optional)
 * @returns {Object} - { data, loading, error, refresh, isConnected }
 */
export function useApiData(apiFunction, refreshInterval = null) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isConnected, setIsConnected] = useState(false);

  // Fetch data function
  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      
      const result = await apiFunction();
      
      if (result.success) {
        setData(result.data);
        setIsConnected(true);
      } else {
        setError(result.error);
        setIsConnected(false);
      }
    } catch (err) {
      setError(err.message || 'Failed to fetch data');
      setIsConnected(false);
    } finally {
      setLoading(false);
    }
  }, [apiFunction]);

  // Manual refresh function
  const refresh = useCallback(() => {
    fetchData();
  }, [fetchData]);

  // Initial data fetch
  useEffect(() => {
    fetchData();
  }, [fetchData]);

  // Auto-refresh if interval is provided
  useEffect(() => {
    if (!refreshInterval) return;

    const intervalId = setInterval(fetchData, refreshInterval);
    
    return () => clearInterval(intervalId);
  }, [fetchData, refreshInterval]);

  return {
    data,
    loading,
    error,
    refresh,
    isConnected
  };
}

/**
 * Hook for testing API connectivity
 * @returns {Object} - { isConnected, testConnection }
 */
export function useApiConnection() {
  const [isConnected, setIsConnected] = useState(false);

  const testConnection = useCallback(async () => {
    try {
      const response = await fetch('http://localhost:5000/');
      const connected = response.ok;
      setIsConnected(connected);
      return connected;
    } catch (error) {
      setIsConnected(false);
      return false;
    }
  }, []);

  useEffect(() => {
    testConnection();
  }, [testConnection]);

  return { isConnected, testConnection };
}
