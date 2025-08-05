#!/usr/bin/env python3
"""
Check table structure to understand column names
"""

from sqlalchemy import create_engine, text
import pandas as pd

def check_table_structure():
    engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
    
    # Check golden_cotations
    print("=== GOLDEN COTATIONS COLUMNS ===")
    try:
        result = engine.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'golden_cotations' 
            ORDER BY ordinal_position
        """))
        for row in result.fetchall():
            print(f"  {row[0]}: {row[1]}")
    except Exception as e:
        print(f"Error checking golden_cotations: {e}")
    
    # Check golden_indices
    print("\n=== GOLDEN INDICES COLUMNS ===")
    try:
        result = engine.execute(text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'golden_indices' 
            ORDER BY ordinal_position
        """))
        for row in result.fetchall():
            print(f"  {row[0]}: {row[1]}")
    except Exception as e:
        print(f"Error checking golden_indices: {e}")
    
    # Check sample data
    print("\n=== SAMPLE DATA ===")
    try:
        cotations_sample = pd.read_sql("SELECT * FROM golden_cotations LIMIT 3", engine)
        print("Golden cotations sample:")
        print(cotations_sample.head())
        
        indices_sample = pd.read_sql("SELECT * FROM golden_indices LIMIT 3", engine)
        print("\nGolden indices sample:")
        print(indices_sample.head())
    except Exception as e:
        print(f"Error reading sample data: {e}")

if __name__ == "__main__":
    check_table_structure() 