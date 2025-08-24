from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
import logging

# Simplified database configuration - no complex imports
DB_CONFIG = {
    'username': 'postgres',
    'password': 'Mahdi1574$',
    'host': 'localhost',
    'port': '5432',
    'database': 'pfe_database'
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app instance
app = Flask(__name__)
CORS(app)  # Enable CORS for frontend communication

# Simple test route
@app.route('/', methods=['GET'])
def root():
    """Root endpoint to test if Flask is working"""
    return jsonify({
        'message': 'Bourse de Tunis ETL Platform Backend is running!',
        'status': 'active',
        'timestamp': datetime.now().isoformat()
    }), 200

# Database connection function
def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['username'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

# Test database connection
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint to test database connection"""
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            cursor.close()
            conn.close()
            
            return jsonify({
                'status': 'healthy',
                'database': 'connected',
                'timestamp': datetime.now().isoformat(),
                'db_version': db_version[0] if db_version else 'unknown'
            }), 200
        else:
            return jsonify({
                'status': 'unhealthy',
                'database': 'disconnected',
                'timestamp': datetime.now().isoformat(),
                'error': 'Could not establish database connection'
            }), 500
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({
            'status': 'unhealthy',
            'database': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 500

# Get ETL pipeline status
@app.route('/api/etl/status', methods=['GET'])
def get_etl_status():
    """Get current ETL pipeline status from database"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query to get ETL pipeline status
        # This will be customized based on your actual database schema
        cursor.execute("""
            SELECT 
                'Bronze Layer' as name,
                'running' as status,
                85 as progress,
                1250000 as records,
                '2h 15m' as duration,
                0 as errors,
                2 as warnings
            UNION ALL
            SELECT 
                'Silver Layer' as name,
                'running' as status,
                67 as progress,
                980000 as records,
                '1h 45m' as duration,
                0 as errors,
                1 as warnings
            UNION ALL
            SELECT 
                'Golden Layer' as name,
                'idle' as status,
                0 as progress,
                0 as records,
                '0m' as duration,
                0 as errors,
                0 as warnings
            UNION ALL
            SELECT 
                'Diamond Layer' as name,
                'idle' as status,
                0 as progress,
                0 as records,
                '0m' as duration,
                0 as errors,
                0 as warnings
        """)
        
        pipelines = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': pipelines,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"ETL status error: {e}")
        return jsonify({
            'error': 'Failed to fetch ETL status',
            'details': str(e)
        }), 500

# Get scraping status
@app.route('/api/scraping/status', methods=['GET'])
def get_scraping_status():
    """Get current scraping job status from database"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query to get scraping status
        # This will be customized based on your actual database schema
        cursor.execute("""
            SELECT 
                'BVMT Cotations' as name,
                'completed' as status,
                '2 hours ago' as last_run,
                15000 as records,
                '45m' as duration,
                0 as errors,
                1 as warnings
        """)
        
        scraping_jobs = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': scraping_jobs,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Scraping status error: {e}")
        return jsonify({
            'error': 'Failed to fetch scraping status',
            'details': str(e)
        }), 500

# Get system statistics
@app.route('/api/system/stats', methods=['GET'])
def get_system_stats():
    """Get system statistics (CPU, memory, etc.)"""
    try:
        # For now, return mock data
        # Later this can be connected to actual system monitoring
        stats = {
            'totalRecords': 1250000,
            'activePipelines': 2,
            'cpuUsage': 45,
            'memoryUsage': 62
        }
        
        return jsonify({
            'status': 'success',
            'data': stats,
            'timestamp': datetime.now().isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"System stats error: {e}")
        return jsonify({
            'error': 'Failed to fetch system stats',
            'details': str(e)
        }), 500

# Test the app can be imported
if __name__ == '__main__':
    print("🔗 Testing Flask app import...")
    print("✅ Flask app created successfully")
    print("🔗 Registered Routes:")
    for rule in app.url_map.iter_rules():
        print(f"   {rule.endpoint}: {rule.rule}")
    print("\n🚀 Use 'python server.py' to start the server with Waitress")
