-- 🗄️ Database initialization script for Medallion ETL Project

-- Create database if not exists (handled by Docker environment variables)

-- Connect to the medallion_etl database
\c medallion_etl;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create indices for better performance
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);

-- Create ETL processing logs table
CREATE TABLE IF NOT EXISTS etl_logs (
    id SERIAL PRIMARY KEY,
    layer VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    rows_processed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create scraping logs table
CREATE TABLE IF NOT EXISTS scraping_logs (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    year INTEGER,
    data_type VARCHAR(50), -- 'indices' or 'cotations'
    status VARCHAR(50),
    file_path TEXT,
    rows_scraped INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert a default admin user (password: 'admin123')
-- Note: In production, change this password immediately
INSERT INTO users (username, email, password_hash) 
VALUES (
    'admin',
    'admin@medallion-etl.com',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBGQ6qdKWw0QMO'
) ON CONFLICT (username) DO NOTHING;

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO medallion_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO medallion_user;

-- Display completion message
DO $$
BEGIN
    RAISE NOTICE '✅ Database initialization completed successfully!';
    RAISE NOTICE '📊 Tables created: users, etl_logs, scraping_logs';
    RAISE NOTICE '👤 Default admin user created (username: admin, password: admin123)';
    RAISE NOTICE '⚠️  Remember to change the default password in production!';
END $$;
