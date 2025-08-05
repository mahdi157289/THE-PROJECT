#!/usr/bin/env python3
"""
Check for Missing Records
=========================
Compare expected vs actual records in diamond tables
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_missing_records():
    """Check for missing records in diamond tables"""
    engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
    
    try:
        with engine.connect() as conn:
            print("🔍 CHECKING FOR MISSING RECORDS")
            print("=" * 50)
            
            # Check diamond_metrics table
            metrics_query = "SELECT COUNT(*) as count FROM diamond_metrics"
            metrics_count = pd.read_sql(metrics_query, conn)
            metrics_total = metrics_count['count'].iloc[0]
            print(f"📊 Diamond Metrics: {metrics_total} records")
            
            # Check diamond_advanced_results table
            advanced_query = "SELECT COUNT(*) as count FROM diamond_advanced_results"
            advanced_count = pd.read_sql(advanced_query, conn)
            advanced_total = advanced_count['count'].iloc[0]
            print(f"📈 Diamond Advanced Results: {advanced_total} records")
            
            # Calculate total
            actual_total = metrics_total + advanced_total
            expected_total = 608
            missing = expected_total - actual_total
            
            print(f"\n📋 SUMMARY:")
            print(f"   Expected: {expected_total} records")
            print(f"   Actual: {actual_total} records")
            print(f"   Missing: {missing} records")
            
            if missing > 0:
                print(f"\n❌ Missing {missing} records!")
                
                # Check what categories we have
                print(f"\n🔍 DETAILED ANALYSIS:")
                
                # Check metrics by category
                if metrics_total > 0:
                    category_query = """
                    SELECT category, COUNT(*) as count 
                    FROM diamond_metrics 
                    GROUP BY category
                    ORDER BY count DESC
                    """
                    categories = pd.read_sql(category_query, conn)
                    print(f"\n📊 Diamond Metrics by Category:")
                    print(categories.to_string())
                
                # Check advanced results by category
                if advanced_total > 0:
                    adv_category_query = """
                    SELECT test_category, COUNT(*) as count 
                    FROM diamond_advanced_results 
                    GROUP BY test_category
                    ORDER BY count DESC
                    """
                    adv_categories = pd.read_sql(adv_category_query, conn)
                    print(f"\n📈 Diamond Advanced Results by Category:")
                    print(adv_categories.to_string())
                
                # Check recent execution dates
                print(f"\n📅 Recent Execution Dates:")
                
                if metrics_total > 0:
                    metrics_dates_query = """
                    SELECT execution_time::date as date, COUNT(*) as count 
                    FROM diamond_metrics 
                    GROUP BY execution_time::date
                    ORDER BY date DESC
                    LIMIT 5
                    """
                    metrics_dates = pd.read_sql(metrics_dates_query, conn)
                    print(f"\nDiamond Metrics execution dates:")
                    print(metrics_dates.to_string())
                
                if advanced_total > 0:
                    adv_dates_query = """
                    SELECT execution_date, COUNT(*) as count 
                    FROM diamond_advanced_results 
                    GROUP BY execution_date
                    ORDER BY execution_date DESC
                    LIMIT 5
                    """
                    adv_dates = pd.read_sql(adv_dates_query, conn)
                    print(f"\nDiamond Advanced Results execution dates:")
                    print(adv_dates.to_string())
                
            else:
                print(f"\n✅ All records present!")
            
            # Check if there are any failed or error records
            print(f"\n🔍 CHECKING FOR ERROR RECORDS:")
            
            if metrics_total > 0:
                error_query = """
                SELECT status, COUNT(*) as count 
                FROM diamond_metrics 
                GROUP BY status
                """
                errors = pd.read_sql(error_query, conn)
                print(f"\nDiamond Metrics status:")
                print(errors.to_string())
            
            return True
            
    except Exception as e:
        logger.error(f"Check failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = check_missing_records()
    
    if success:
        print("\n✅ Check completed!")
    else:
        print("\n❌ Check failed!") 