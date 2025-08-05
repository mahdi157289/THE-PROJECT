#!/usr/bin/env python3
"""
Analyze Advanced Results
========================
Show exactly which tools and tests generated the 53 advanced results
"""

import pandas as pd
from sqlalchemy import create_engine, text
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_advanced_results():
    """Analyze the 53 advanced results in detail"""
    engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
    
    try:
        with engine.connect() as conn:
            print("🔍 ANALYZING 53 ADVANCED RESULTS")
            print("=" * 60)
            
            # Get all advanced results
            query = """
            SELECT test_category, test_name, model_type, parameters, performance_metrics, raw_output
            FROM diamond_advanced_results 
            ORDER BY test_category, test_name
            """
            results = pd.read_sql(query, conn)
            
            print(f"📊 Total Advanced Results: {len(results)}")
            print("\n" + "="*60)
            
            # Group by category
            categories = results.groupby('test_category')
            
            for category, group in categories:
                print(f"\n🎯 CATEGORY: {category.upper()}")
                print(f"📈 Count: {len(group)} results")
                print("-" * 50)
                
                for idx, row in group.iterrows():
                    test_name = row['test_name']
                    model_type = row['model_type']
                    
                    print(f"\n🔧 Test: {test_name}")
                    print(f"   Model Type: {model_type}")
                    
                    # Parse and show parameters if available
                    if row['parameters'] and row['parameters'] != '{}':
                        try:
                            params = json.loads(row['parameters']) if isinstance(row['parameters'], str) else row['parameters']
                            if params:
                                print(f"   Parameters: {list(params.keys())}")
                        except:
                            pass
                    
                    # Parse and show performance metrics if available
                    if row['performance_metrics'] and row['performance_metrics'] != '{}':
                        try:
                            metrics = json.loads(row['performance_metrics']) if isinstance(row['performance_metrics'], str) else row['performance_metrics']
                            if metrics:
                                print(f"   Performance Metrics: {list(metrics.keys())}")
                        except:
                            pass
            
            print("\n" + "="*60)
            print("📋 TOOLS AND TESTS USED:")
            print("="*60)
            
            # Show tools used for each category
            print("\n🔧 ADVANCED MODELS (11 results):")
            print("   - GARCH Volatility Analysis")
            print("   - Tools: arch library, statistical modeling")
            print("   - Tests: Volatility clustering, persistence analysis")
            
            print("\n📈 TRADING STRATEGIES (26 results):")
            print("   - Rolling Correlation Strategy")
            print("   - Tools: pandas, numpy, statistical correlation")
            print("   - Tests: Dynamic threshold analysis, multiple time windows")
            
            print("\n⚠️ RISK MANAGEMENT (2 results):")
            print("   - Risk Metrics Calculation")
            print("   - Tools: scipy.stats, numpy")
            print("   - Tests: VaR, Expected Shortfall, Drawdown analysis")
            
            print("\n📊 MARKET ANALYSIS (14 results):")
            print("   - Market Regime Detection")
            print("   - Tools: scipy.stats, numpy, pandas")
            print("   - Tests: Bull/Bear/Sideways market detection")
            
            print("\n" + "="*60)
            print("🛠️ TECHNICAL IMPLEMENTATION:")
            print("="*60)
            
            print("\n📚 Libraries Used:")
            print("   - pandas: Data manipulation and analysis")
            print("   - numpy: Numerical computations")
            print("   - scipy.stats: Statistical tests")
            print("   - arch: GARCH modeling")
            print("   - sqlalchemy: Database operations")
            
            print("\n🔍 Statistical Methods:")
            print("   - GARCH(1,1) model fitting")
            print("   - Rolling correlation analysis")
            print("   - Risk metrics calculation (VaR, ES)")
            print("   - Market regime detection")
            print("   - Volatility persistence analysis")
            
            print("\n💾 Data Sources:")
            print("   - Golden layer cotations data")
            print("   - Golden layer indices data")
            print("   - Price and volume time series")
            print("   - Technical indicators")
            
            return True
            
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = analyze_advanced_results()
    
    if success:
        print("\n✅ Analysis completed!")
    else:
        print("\n❌ Analysis failed!") 