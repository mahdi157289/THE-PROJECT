#!/usr/bin/env python3
"""
Setup users table in PostgreSQL database
"""

import psycopg2
import bcrypt

# Database configuration
DB_CONFIG = {
    'username': 'postgres',
    'password': 'Mahdi1574$',
    'host': 'localhost',
    'port': '5432',
    'database': 'pfe_database'
}

def create_users_table():
    """Create users table and insert default admin user"""
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['username'],
            password=DB_CONFIG['password'],
            port=DB_CONFIG['port']
        )
        
        cursor = conn.cursor()
        
        # Create users table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
        """
        
        # Create indexes
        create_indexes_sql = """
        CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
        """
        
        # Hash password for admin user
        password = 'admin123'
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Insert admin user
        insert_admin_sql = """
        INSERT INTO users (username, email, password_hash) 
        VALUES (%s, %s, %s)
        ON CONFLICT (username) DO NOTHING;
        """
        
        print("🔧 Creating users table...")
        cursor.execute(create_table_sql)
        
        print("🔧 Creating indexes...")
        cursor.execute(create_indexes_sql)
        
        print("🔧 Inserting admin user...")
        cursor.execute(insert_admin_sql, ('admin', 'admin@bourse-tunis.com', password_hash))
        
        # Commit changes
        conn.commit()
        
        print("✅ Users table created successfully!")
        print("✅ Default admin user created:")
        print("   Username: admin")
        print("   Password: admin123")
        print("   Email: admin@bourse-tunis.com")
        
        # Verify table creation
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        print(f"✅ Total users in database: {user_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating users table: {e}")
        if conn:
            conn.rollback()
            conn.close()

if __name__ == '__main__':
    print("🚀 Setting up users table for authentication...")
    create_users_table()
    print("�� Setup complete!")

