/**
 * API Service Layer for Bourse de Tunis ETL Platform
 * Centralized HTTP client for backend communication
 */

// API Configuration
const API_CONFIG = {
  baseURL: 'http://localhost:5000',
  timeout: 10000, // 10 seconds
  endpoints: {
    health: '/api/health',
    etlStatus: '/api/etl/status',
    scrapingStatus: '/api/scraping/status',
    systemStats: '/api/system/stats'
  }
};

/**
 * Generic HTTP request function with error handling
 * @param {string} endpoint - API endpoint
 * @param {Object} options - Fetch options
 * @returns {Promise<Object>} - Response data or error
 */
async function apiRequest(endpoint, options = {}) {
  const url = `${API_CONFIG.baseURL}${endpoint}`;
  
  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers
      },
      timeout: API_CONFIG.timeout,
      ...options
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    return { success: true, data };
    
  } catch (error) {
    console.error(`API Request failed for ${endpoint}:`, error);
    return { 
      success: false, 
      error: error.message || 'Network error occurred',
      endpoint 
    };
  }
}

/**
 * Health check endpoint
 * @returns {Promise<Object>} - Health status
 */
export async function checkHealth() {
  return apiRequest(API_CONFIG.endpoints.health);
}

/**
 * Get ETL pipeline status
 * @returns {Promise<Object>} - ETL pipeline data
 */
export async function getETLStatus() {
  return apiRequest(API_CONFIG.endpoints.etlStatus);
}

/**
 * Get scraping job status
 * @returns {Promise<Object>} - Scraping job data
 */
export async function getScrapingStatus() {
  return apiRequest(API_CONFIG.endpoints.scrapingStatus);
}

/**
 * Get system statistics
 * @returns {Promise<Object>} - System stats data
 */
export async function getSystemStats() {
  return apiRequest(API_CONFIG.endpoints.systemStats);
}

/**
 * Test API connectivity
 * @returns {Promise<boolean>} - Connection status
 */
export async function testAPIConnection() {
  try {
    const response = await fetch(`${API_CONFIG.baseURL}/`);
    return response.ok;
  } catch (error) {
    console.error('API connection test failed:', error);
    return false;
  }
}

// Export configuration for components that need it
export { API_CONFIG };
