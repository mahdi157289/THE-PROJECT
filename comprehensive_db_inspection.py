#!/usr/bin/env python3
"""
Comprehensive Database Inspection
================================
Check all tables in the database and their contents
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_database():
    """Comprehensive database inspection"""
    engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
    
    try:
        with engine.connect() as conn:
            # Get all table names
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, conn)
            
            print("=== DATABASE TABLES ===")
            print(f"Found {len(tables)} tables:")
            for idx, table in tables.iterrows():
                print(f"  {idx+1}. {table['table_name']}")
            
            print("\n" + "="*50)
            
            # Check each table
            for idx, table in tables.iterrows():
                table_name = table['table_name']
                print(f"\n=== TABLE: {table_name.upper()} ===")
                
                # Get row count
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_result = pd.read_sql(count_query, conn)
                row_count = count_result['count'].iloc[0]
                print(f"Total rows: {row_count}")
                
                if row_count > 0:
                    # Get column info
                    columns_query = f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                    """
                    columns = pd.read_sql(columns_query, conn)
                    print(f"Columns ({len(columns)}):")
                    for _, col in columns.iterrows():
                        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                        print(f"  - {col['column_name']}: {col['data_type']} ({nullable})")
                    
                    # Get sample data
                    sample_query = f"SELECT * FROM {table_name} LIMIT 3"
                    sample_data = pd.read_sql(sample_query, conn)
                    print(f"\nSample data (first 3 rows):")
                    print(sample_data.to_string())
                    
                    # For diamond tables, show more details
                    if 'diamond' in table_name.lower():
                        print(f"\n--- DIAMOND TABLE ANALYSIS ---")
                        
                        # Check for non-null values in key columns
                        if 'test_category' in sample_data.columns:
                            category_query = f"""
                            SELECT test_category, COUNT(*) as count 
                            FROM {table_name} 
                            GROUP BY test_category
                            """
                            categories = pd.read_sql(category_query, conn)
                            print("Test categories distribution:")
                            print(categories.to_string())
                        
                        if 'execution_date' in sample_data.columns:
                            date_query = f"""
                            SELECT execution_date, COUNT(*) as count 
                            FROM {table_name} 
                            GROUP BY execution_date
                            ORDER BY execution_date DESC
                            LIMIT 5
                            """
                            dates = pd.read_sql(date_query, conn)
                            print("\nRecent execution dates:")
                            print(dates.to_string())
                
                print("-" * 50)
                
    except Exception as e:
        logger.error(f"Database inspection failed: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    print("🔍 COMPREHENSIVE DATABASE INSPECTION")
    print("=" * 50)
    success = inspect_database()
    
    if success:
        print("\n✅ Database inspection completed!")
    else:
        print("\n❌ Database inspection failed!") 