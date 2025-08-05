#!/usr/bin/env python3
"""
Script to manually insert diamond metrics data to database
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging
from pathlib import Path
import glob

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_latest_metrics_file():
    """Get the latest metrics parquet file"""
    data_dir = Path('data/diamond')
    if not data_dir.exists():
        logger.error("Data directory not found")
        return None
    
    # Find all parquet files
    parquet_files = list(data_dir.glob('validation_metrics_*.parquet'))
    if not parquet_files:
        logger.error("No metrics files found")
        return None
    
    # Get the latest file
    latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Found latest metrics file: {latest_file}")
    return latest_file

def insert_metrics_to_database(file_path: Path):
    """Insert metrics from parquet file to database"""
    try:
        # Read the parquet file
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} records from {file_path}")
        
        # Create database connection
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        
        # Insert data in batches
        batch_size = 50
        total_inserted = 0
        
        with engine.connect() as conn:
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                
                # Insert batch
                batch_df.to_sql('diamond_metrics', conn, if_exists='append', 
                               index=False, method='multi')
                
                total_inserted += len(batch_df)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_df)} records")
            
            conn.commit()
            logger.info(f"Successfully inserted {total_inserted} records to diamond_metrics table")
            return True
            
    except Exception as e:
        logger.error(f"Failed to insert data: {str(e)}")
        return False

def verify_database_data():
    """Verify that data was inserted correctly"""
    try:
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        with engine.connect() as conn:
            # Get count
            result = conn.execute(text("SELECT COUNT(*) as count FROM diamond_metrics"))
            count = result.fetchone()[0]
            logger.info(f"Total records in diamond_metrics: {count}")
            
            # Get sample data
            result = conn.execute(text("SELECT test_name, category, status FROM diamond_metrics LIMIT 5"))
            sample_data = result.fetchall()
            logger.info("Sample data:")
            for row in sample_data:
                logger.info(f"  {row}")
            
            return True
            
    except Exception as e:
        logger.error(f"Failed to verify data: {str(e)}")
        return False

def main():
    """Main function"""
    logger.info("Starting database insert process...")
    
    # Get latest metrics file
    metrics_file = get_latest_metrics_file()
    if not metrics_file:
        return False
    
    # Insert data to database
    success = insert_metrics_to_database(metrics_file)
    if not success:
        return False
    
    # Verify the data
    verify_database_data()
    
    logger.info("Database insert process completed successfully!")
    return True

if __name__ == "__main__":
    main() 