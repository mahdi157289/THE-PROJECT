# 🚀 Medallion ETL Project - Deployment Guide

## 📋 Table of Contents
- [Quick Start](#quick-start)
- [Local Docker Testing](#local-docker-testing)
- [Railway.app Deployment](#railwayapp-deployment)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Git repository access
- Railway.app account (for cloud deployment)

### 1. Local Testing with Docker

```bash
# Clone and navigate to project
git clone <your-repo-url>
cd medallion_etl_project/etl_web_platform

# Copy environment template
cp env.example .env

# Edit .env with your values
nano .env

# Build and run with Docker Compose
docker-compose up --build
```

**Access Points:**
- Frontend: http://localhost
- Backend API: http://localhost:5000
- Database: localhost:5432

### 2. Railway.app Deployment

#### Step 1: Prepare Repository
```bash
# Ensure all files are committed
git add .
git commit -m "Add Docker configuration for deployment"
git push origin main
```

#### Step 2: Deploy Backend
1. Go to [Railway.app](https://railway.app)
2. Create new project
3. Connect GitHub repository
4. Select `etl_web_platform` folder
5. Add PostgreSQL database service
6. Set environment variables (see below)

#### Step 3: Deploy Frontend
1. Create new service in same project
2. Set build path to `frontend/`
3. Configure domain

## 🔧 Environment Variables

### Required for Production:

```bash
# Database (Auto-provided by Railway PostgreSQL)
DATABASE_URL=postgresql://user:pass@host:port/db

# Security (Generate strong keys!)
SECRET_KEY=your-256-bit-secret-key
JWT_SECRET_KEY=your-jwt-secret-key

# CORS (Add your Railway domains)
CORS_ORIGINS=https://your-app.railway.app,https://your-frontend.railway.app

# App Settings
FLASK_ENV=production
DEBUG=False
PORT=5000
```

### Optional:
```bash
# Power BI Integration
POWERBI_CLIENT_ID=your-client-id
POWERBI_CLIENT_SECRET=your-client-secret
POWERBI_TENANT_ID=your-tenant-id
POWERBI_WORKSPACE_ID=your-workspace-id

# Monitoring
SENTRY_DSN=your-sentry-dsn
LOG_LEVEL=INFO
```

## 🐳 Docker Commands

### Local Development
```bash
# Build services
docker-compose build

# Start services
docker-compose up

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Clean rebuild
docker-compose down -v
docker-compose build --no-cache
docker-compose up
```

### Production Build Testing
```bash
# Build production images
docker build -t medallion-frontend ./frontend
docker build -t medallion-backend ./backend

# Test backend
docker run -p 5000:5000 medallion-backend

# Test frontend
docker run -p 80:80 medallion-frontend
```

## 🌐 Railway Deployment Steps

### 1. Database Setup
1. Add PostgreSQL service to Railway project
2. Note the DATABASE_URL from Railway dashboard
3. Database will auto-initialize with `database/init.sql`

### 2. Backend Deployment
1. Connect repository to Railway
2. Set root directory to `etl_web_platform`
3. Railway will detect Dockerfile in backend/
4. Add environment variables
5. Deploy

### 3. Frontend Deployment
1. Create new service
2. Set build command: `cd frontend && npm run build`
3. Set start command: `serve -s dist -p $PORT`
4. Or use Dockerfile approach

### 4. Domain Configuration
1. Get Railway-provided URLs
2. Update CORS_ORIGINS in backend
3. Update API URLs in frontend
4. Add custom domain (optional)

## 🔍 Health Checks

### Backend Health
```bash
curl http://your-backend-url/
# Should return: "Medallion ETL Backend API is running!"
```

### Frontend Health
```bash
curl http://your-frontend-url/health
# Should return: "healthy"
```

### Database Health
```bash
# From backend container
pg_isready -h database -p 5432 -U medallion_user
```

## 🐛 Troubleshooting

### Common Issues

#### 1. Database Connection Failed
```bash
# Check DATABASE_URL format
postgresql://user:password@host:port/database

# Verify database service is running
docker-compose ps
```

#### 2. CORS Errors
```bash
# Add frontend URL to CORS_ORIGINS
CORS_ORIGINS=https://frontend.railway.app,http://localhost:3000
```

#### 3. Build Failures
```bash
# Clear Docker cache
docker system prune -a

# Check logs
docker-compose logs backend
docker-compose logs frontend
```

#### 4. Port Conflicts
```bash
# Check what's using port 5000
lsof -i :5000

# Kill process
kill -9 <PID>
```

### Railway-Specific Issues

#### 1. Build Context
- Ensure `railway.json` points to correct Dockerfile
- Check build logs in Railway dashboard

#### 2. Environment Variables
- Double-check all required variables are set
- Ensure DATABASE_URL includes SSL parameters if needed

#### 3. Memory Limits
- Railway free tier has memory limits
- Optimize Docker images if needed

## 📊 Monitoring

### Logs
```bash
# Docker logs
docker-compose logs -f --tail=100

# Railway logs
# Available in Railway dashboard
```

### Performance
- Monitor Railway metrics dashboard
- Set up alerts for downtime
- Use health check endpoints

## 🔄 Updates and Maintenance

### Code Updates
```bash
# Deploy new version
git push origin main
# Railway auto-deploys on push
```

### Database Migrations
```bash
# Add migration scripts to database/migrations/
# Run via Railway CLI or admin panel
```

### Backup Strategy
- Railway auto-backs up PostgreSQL
- Export data regularly for safety
- Store environment variables securely

## 📚 Additional Resources

- [Railway Documentation](https://docs.railway.app)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [PostgreSQL on Railway](https://docs.railway.app/databases/postgresql)
- [Flask Deployment Guide](https://flask.palletsprojects.com/en/2.0.x/deploying/)

---

🎉 **Your Medallion ETL project is now ready for deployment!**
