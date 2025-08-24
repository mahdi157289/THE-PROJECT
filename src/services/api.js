import axios from 'axios'

// Create axios instance with base configuration
const api = axios.create({
  baseURL: process.env.REACT_APP_API_URL || 'http://localhost:5000/api',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for adding auth tokens
api.interceptors.request.use(
  (config) => {
    // TODO: Add authentication token if needed
    return config
  },
  (error) => {
    return Promise.reject(error)
  }
)

// Response interceptor for handling errors
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    console.error('API Error:', error)
    return Promise.reject(error)
  }
)

// ETL Pipeline API endpoints
export const etlAPI = {
  // Get pipeline status for all layers
  getPipelineStatus: () => api.get('/etl/pipeline-status'),
  
  // Start a specific pipeline layer
  startPipeline: (layer) => api.post(`/etl/start/${layer}`),
  
  // Stop a specific pipeline layer
  stopPipeline: (layer) => api.post(`/etl/stop/${layer}`),
  
  // Restart a specific pipeline layer
  restartPipeline: (layer) => api.post(`/etl/restart/${layer}`),
  
  // Get pipeline logs
  getPipelineLogs: (layer, limit = 100) => api.get(`/etl/logs/${layer}?limit=${limit}`),
  
  // Get pipeline metrics
  getPipelineMetrics: () => api.get('/etl/metrics'),
}

// Scraping API endpoints
export const scrapingAPI = {
  // Get scraping status for all types
  getScrapingStatus: () => api.get('/scraping/status'),
  
  // Start scraping for a specific type
  startScraping: (type) => api.post(`/scraping/start/${type}`),
  
  // Stop scraping for a specific type
  stopScraping: (type) => api.post(`/scraping/stop/${type}`),
  
  // Get scraping history
  getScrapingHistory: (type, limit = 50) => api.get(`/scraping/history/${type}?limit=${limit}`),
  
  // Get scraped files list
  getScrapedFiles: (type) => api.get(`/scraping/files/${type}`),
}

// Analytics API endpoints
export const analyticsAPI = {
  // Get market metrics
  getMarketMetrics: () => api.get('/analytics/market-metrics'),
  
  // Get ML model predictions
  getMLPredictions: () => api.get('/analytics/ml-predictions'),
  
  // Get data quality metrics
  getDataQualityMetrics: () => api.get('/analytics/data-quality'),
  
  // Get historical analytics data
  getHistoricalData: (startDate, endDate) => api.get(`/analytics/historical?start=${startDate}&end=${endDate}`),
}

// Power BI API endpoints
export const powerBIIPI = {
  // Get available dashboards
  getDashboards: () => api.get('/powerbi/dashboards'),
  
  // Get dashboard details
  getDashboard: (id) => api.get(`/powerbi/dashboards/${id}`),
  
  // Get dashboard embed token
  getEmbedToken: (id) => api.get(`/powerbi/dashboards/${id}/embed-token`),
  
  // Get dashboard thumbnail
  getThumbnail: (id) => api.get(`/powerbi/dashboards/${id}/thumbnail`),
}

// System health API endpoints
export const systemAPI = {
  // Get system health metrics
  getSystemHealth: () => api.get('/system/health'),
  
  // Get system logs
  getSystemLogs: (limit = 100) => api.get(`/system/logs?limit=${limit}`),
  
  // Get database connection status
  getDatabaseStatus: () => api.get('/system/database-status'),
}

// Database API endpoints (for direct data access)
export const databaseAPI = {
  // Get table row counts
  getTableCounts: () => api.get('/database/table-counts'),
  
  // Get sample data from a table
  getSampleData: (table, limit = 100) => api.get(`/database/sample/${table}?limit=${limit}`),
  
  // Execute custom query (with proper validation)
  executeQuery: (query) => api.post('/database/query', { query }),
}

export default api
