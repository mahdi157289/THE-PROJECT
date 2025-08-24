# Bourse de Tunis ETL Platform - Backend

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Backend
```bash
python run.py
```

The backend will start on `http://localhost:5000`

## 📊 API Endpoints

### Health Check
- **GET** `/api/health` - Test database connection

### ETL Pipeline
- **GET** `/api/etl/status` - Get ETL pipeline status

### Scraping
- **GET** `/api/scraping/status` - Get scraping job status

### System
- **GET** `/api/system/stats` - Get system statistics

## 🔧 Configuration

The backend automatically uses your existing database configuration from `config/settings.py`:
- Host: `localhost`
- Database: `pfe_database`
- Username: `postgres`
- Password: `Mahdi1574$`
- Port: `5432`

## 🐛 Troubleshooting

### Database Connection Issues
1. Ensure PostgreSQL is running
2. Check database credentials in `config/settings.py`
3. Verify database exists: `pfe_database`

### Import Errors
1. Make sure you're in the `backend/` directory
2. Install all requirements: `pip install -r requirements.txt`
3. Check Python path configuration

## 📁 Project Structure
```
backend/
├── app.py          # Main Flask application
├── run.py          # Startup script
├── requirements.txt # Python dependencies
└── README.md       # This file
```

## 🔗 Frontend Integration

The backend is configured with CORS to allow communication with your React frontend running on `http://localhost:5173`.
