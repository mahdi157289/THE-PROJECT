#!/usr/bin/env python3
"""
Script to create the diamond_metrics table with correct schema
"""

from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_diamond_metrics_table():
    """Create the diamond_metrics table with correct schema"""
    try:
        # Create database connection
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        
        # Create table with correct schema
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS diamond_metrics (
            id SERIAL PRIMARY KEY,
            test_name VARCHAR(255),
            category VARCHAR(100),
            execution_time TIMESTAMP,
            status VARCHAR(50),
            statistic_value DOUBLE PRECISION,
            simple_value TEXT,
            p_value DOUBLE PRECISION,
            is_significant BOOLEAN,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with engine.connect() as conn:
            # Drop existing table if it exists
            conn.execute(text("DROP TABLE IF EXISTS diamond_metrics CASCADE"))
            logger.info("Dropped existing diamond_metrics table")
            
            # Create new table
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info("Created diamond_metrics table successfully")
            
            # Verify table structure
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'diamond_metrics'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            logger.info("Table structure:")
            for col in columns:
                logger.info(f"  {col[0]}: {col[1]}")
            
            return True
            
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        return False

def main():
    """Main function"""
    logger.info("Creating diamond_metrics table...")
    success = create_diamond_metrics_table()
    
    if success:
        logger.info("Table creation completed successfully!")
    else:
        logger.error("Table creation failed!")
    
    return success

if __name__ == "__main__":
    main() 