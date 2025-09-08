# ETL Web Platform - Frontend

A modern React-based web platform for monitoring and controlling your Medallion Architecture ETL pipeline, BVMT scraping operations, and Power BI dashboards.

## 🚀 Features

### **Dashboard Overview**
- Real-time pipeline status monitoring
- System health metrics (CPU, Memory, Disk)
- Quick action buttons for common operations
- Responsive design for all devices

### **ETL Pipeline Management**
- Monitor Bronze, Silver, Golden, and Diamond layers
- Start/Stop/Restart individual pipeline layers
- Real-time progress tracking
- Pipeline logs and error monitoring

### **BVMT Scraping Monitor**
- Monitor cotations and indices scraping jobs
- Start/Stop scraping operations
- View scraped files and job history
- Real-time job status updates

### **Power BI Integration**
- Access existing Power BI dashboards
- Embed dashboards directly in the platform
- Dashboard management and status monitoring

### **Financial Analytics**
- Market metrics and trends
- ML model predictions and performance
- Data quality metrics
- Historical data analysis

## 🛠️ Technology Stack

- **Frontend Framework:** React 18 with Vite
- **Styling:** Tailwind CSS
- **Icons:** Heroicons
- **Animations:** Framer Motion
- **HTTP Client:** Axios
- **Routing:** React Router DOM
- **Notifications:** React Hot Toast
- **Date Handling:** date-fns
- **Forms:** React Hook Form

## 📁 Project Structure

```
src/
├── components/          # Reusable UI components
│   └── Layout.jsx      # Main layout with navigation
├── pages/              # Page components
│   ├── Dashboard.jsx   # Main dashboard
│   ├── ETLPipeline.jsx # ETL pipeline management
│   ├── ScrapingMonitor.jsx # BVMT scraping monitor
│   ├── PowerBIDashboards.jsx # Power BI integration
│   └── Analytics.jsx   # Financial analytics
├── services/           # API services
│   └── api.js         # Backend API integration
├── hooks/              # Custom React hooks
├── utils/              # Utility functions
└── index.css          # Global styles with Tailwind
```

## 🚀 Getting Started

### Prerequisites
- Node.js 16+ 
- npm or yarn

### Installation

1. **Clone the repository**
   ```bash
   cd etl_web_platform/frontend
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Start development server**
   ```bash
   npm run dev
   ```

4. **Open your browser**
   Navigate to `http://localhost:5173`

## 🔧 Configuration

### Environment Variables
Create a `.env` file in the frontend directory:

```env
REACT_APP_API_URL=http://localhost:5000/api
```

### Backend Connection
The frontend expects a Flask backend running on port 5000 with the following API endpoints:

- `/api/etl/*` - ETL pipeline management
- `/api/scraping/*` - Scraping operations
- `/api/analytics/*` - Financial analytics
- `/api/powerbi/*` - Power BI integration
- `/api/system/*` - System health monitoring
- `/api/database/*` - Database operations

## 📱 Responsive Design

The platform is fully responsive and works on:
- Desktop (1024px+)
- Tablet (768px - 1023px)
- Mobile (320px - 767px)

## 🎨 Customization

### Colors
Custom colors are defined in `tailwind.config.js`:
- Primary: Blue shades
- Success: Green shades
- Warning: Yellow shades
- Danger: Red shades

### Components
Reusable component classes in `src/index.css`:
- `.btn-primary` - Primary action buttons
- `.btn-secondary` - Secondary action buttons
- `.card` - Content cards
- `.sidebar-item` - Navigation items

## 🔌 API Integration

The frontend is ready to connect to your existing:
- **PostgreSQL Database** (via your existing connection)
- **ETL Pipeline** (Bronze, Silver, Golden, Diamond layers)
- **BVMT Scraper** (cotations and indices)
- **Power BI Service** (dashboards and reports)
- **ML Models** (predictions and performance)

## 🚧 Development Status

### ✅ Completed
- [x] Project setup with Vite + React
- [x] Tailwind CSS configuration
- [x] Responsive layout and navigation
- [x] All page components
- [x] Mock data and interactions
- [x] API service structure

### 🔄 Next Steps
- [ ] Connect to Flask backend
- [ ] Integrate with PostgreSQL database
- [ ] Connect to Power BI Service
- [ ] Real-time data updates
- [ ] Authentication system
- [ ] Error handling and validation

## 🤝 Contributing

This is a step-by-step development following Scrum methodology. Each sprint focuses on specific functionality:

- **Sprint 1:** Frontend foundation ✅
- **Sprint 2:** Backend integration
- **Sprint 3:** Database connectivity
- **Sprint 4:** Power BI integration
- **Sprint 5:** Testing and optimization

## 📞 Support

For questions or issues:
1. Check the console for error messages
2. Verify backend connectivity
3. Ensure all dependencies are installed
4. Check environment configuration

---

**Built with ❤️ for your ETL Platform** 