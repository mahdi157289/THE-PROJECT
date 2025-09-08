// Power BI Dashboard Configuration
// Update this file with your actual Power BI embed URL and settings

export const powerBIConfig = {
  // Dashboard Information
  title: "PFE - Financial Analytics Dashboard",
  description: "Comprehensive financial analysis with machine learning predictions, risk metrics, and market insights",
  
  // Power BI Embed URL - Replace with your actual embed URL
  // Get this from Power BI Service: Share → Embed code
  embedUrl: "https://app.powerbi.com/reportEmbed?reportId=36d9dcf3-5c50-4499-91dd-49ab1d158970&autoAuth=true&ctid=454921f8-9d1c-4df7-9013-bcd532c4d63a",
  // Dashboard Dimensions
  width: "100%",
  height: "800px",
  minHeight: "600px",
  
  // Dashboard Features
  features: {
    enableActionBar: true,
    enableCopilot: true,
    enableFullScreen: true,
    enableFilterPane: true,
    enableNavigationPane: true
  },
  
  // Data Source Information
  dataSource: {
    type: "PostgreSQL",
    connection: "Live Connection",
    refreshInterval: "Real-time",
    lastUpdated: "Live"
  },
  
  // ETL Integration
  etlIntegration: {
    status: "Connected",
    pipeline: "Medallion Architecture",
    layers: ["Bronze", "Silver", "Golden", "Diamond"],
    dataFlow: "Scraping → ETL → Analytics → Dashboard"
  },
  
  // Dashboard Sections
  sections: [
    {
      name: "Stocks and Deep Learning",
      description: "ML predictions and stock analysis",
      icon: "🤖"
    },
    {
      name: "Statistical Validation Results",
      description: "Advanced statistical testing results",
      icon: "📊"
    },
    {
      name: "GARCH Volatility Analysis",
      description: "Volatility modeling and risk metrics",
      icon: "📈"
    },
    {
      name: "Technical Signals",
      description: "Trading signals and technical analysis",
      icon: "🎯"
    },
    {
      name: "Relationships & Strategy Candidates",
      description: "Strategic analysis and relationships",
      icon: "🔗"
    }
  ]
};

// Instructions for updating the embed URL:
// 1. Go to Power BI Service (app.powerbi.com)
// 2. Open your dashboard
// 3. Click "Share" → "Embed code"
// 4. Copy the iframe src URL
// 5. Replace the embedUrl value above
// 6. Save this file and refresh your web platform

export default powerBIConfig;
