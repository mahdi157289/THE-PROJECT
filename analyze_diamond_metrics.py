#!/usr/bin/env python3
"""
Script to analyze diamond_metrics table and verify completeness of tests
"""

import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_diamond_metrics():
    """Analyze the diamond_metrics table to check test completeness"""
    try:
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        # Get summary of tests by category and status
        summary_query = """
        SELECT test_name, category, status, COUNT(*) as count 
        FROM diamond_metrics 
        GROUP BY test_name, category, status 
        ORDER BY test_name, category
        """
        
        df_summary = pd.read_sql(summary_query, engine)
        print("=== DIAMOND METRICS SUMMARY ===")
        print(df_summary.to_string(index=False))
        
        # Get total count
        total_query = "SELECT COUNT(*) as total FROM diamond_metrics"
        total_count = pd.read_sql(total_query, engine).iloc[0]['total']
        print(f"\nTotal records: {total_count}")
        
        # Get unique tests
        unique_tests_query = """
        SELECT DISTINCT test_name, category 
        FROM diamond_metrics 
        ORDER BY category, test_name
        """
        unique_tests = pd.read_sql(unique_tests_query, engine)
        print(f"\nUnique tests found: {len(unique_tests)}")
        print("\n=== UNIQUE TESTS BY CATEGORY ===")
        for category in unique_tests['category'].unique():
            category_tests = unique_tests[unique_tests['category'] == category]
            print(f"\n{category.upper()}:")
            for _, row in category_tests.iterrows():
                print(f"  - {row['test_name']}")
        
        # Check for expected tests based on financial analysis requirements
        expected_tests = {
            'Statistical Validation': [
                'price_distribution_test',
                'volume_distribution_test', 
                'stationarity_test',
                'normality_test',
                'autocorrelation_test'
            ],
            'Market Analysis': [
                'volatility_analysis',
                'correlation_analysis',
                'cointegration_test',
                'granger_causality_test'
            ],
            'Advanced Models': [
                'garch_model',
                'var_model',
                'deep_learning_model'
            ]
        }
        
        print("\n=== EXPECTED VS ACTUAL TESTS ===")
        for category, expected_list in expected_tests.items():
            actual_tests = unique_tests[unique_tests['category'] == category]['test_name'].tolist()
            missing_tests = set(expected_list) - set(actual_tests)
            extra_tests = set(actual_tests) - set(expected_list)
            
            print(f"\n{category}:")
            print(f"  Expected: {len(expected_list)} tests")
            print(f"  Found: {len(actual_tests)} tests")
            if missing_tests:
                print(f"  Missing: {list(missing_tests)}")
            if extra_tests:
                print(f"  Extra: {list(extra_tests)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to analyze diamond_metrics: {str(e)}")
        return False

def check_diamond_advanced_results():
    """Check the diamond_advanced_results table"""
    try:
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        # Check if table exists and has data
        check_query = "SELECT COUNT(*) as count FROM diamond_advanced_results"
        result = pd.read_sql(check_query, engine)
        count = result.iloc[0]['count']
        
        print(f"\n=== DIAMOND ADVANCED RESULTS ===")
        print(f"Records in diamond_advanced_results: {count}")
        
        if count == 0:
            print("Table is empty - needs to be populated")
            return False
        else:
            # Show sample data
            sample_query = "SELECT * FROM diamond_advanced_results LIMIT 5"
            sample_data = pd.read_sql(sample_query, engine)
            print("\nSample data:")
            print(sample_data.to_string(index=False))
            return True
            
    except Exception as e:
        logger.error(f"Failed to check diamond_advanced_results: {str(e)}")
        return False

def main():
    logger.info("Analyzing diamond_metrics table...")
    success1 = analyze_diamond_metrics()
    
    logger.info("Checking diamond_advanced_results table...")
    success2 = check_diamond_advanced_results()
    
    if success1 and success2:
        logger.info("Analysis completed successfully!")
    else:
        logger.error("Some analysis failed!")

if __name__ == "__main__":
    main() 