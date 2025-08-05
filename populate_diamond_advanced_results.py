#!/usr/bin/env python3
"""
Script to populate diamond_advanced_results table with advanced analysis data
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
from pathlib import Path
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_diamond_advanced_results_table():
    """Create the diamond_advanced_results table if it doesn't exist"""
    try:
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS diamond_advanced_results (
            result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            test_category VARCHAR(100) NOT NULL,
            test_name VARCHAR(255) NOT NULL,
            execution_date DATE NOT NULL DEFAULT CURRENT_DATE,
            model_type VARCHAR(50),
            parameters JSONB,
            performance_metrics JSONB,
            raw_output JSONB,
            notes TEXT
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info("diamond_advanced_results table created/verified successfully")
            return True
            
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        return False

def extract_advanced_results(json_file_path: str) -> list:
    """Extract advanced analysis results from JSON file"""
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        advanced_results = []
        execution_date = datetime.now().date()
        
        # Extract GARCH models
        garch_models = [k for k in data.keys() if k.endswith('_garch')]
        for model_name in garch_models:
            model_data = data[model_name]
            if isinstance(model_data, dict) and 'model' in model_data:
                result = {
                    'test_category': 'Advanced Models',
                    'test_name': f'{model_name}_volatility_analysis',
                    'execution_date': execution_date,
                    'model_type': 'GARCH',
                    'parameters': model_data.get('params', {}),
                    'performance_metrics': {
                        'aic': model_data.get('model_fit', {}).get('aic'),
                        'bic': model_data.get('model_fit', {}).get('bic'),
                        'log_likelihood': model_data.get('model_fit', {}).get('log_likelihood'),
                        'persistence': model_data.get('persistence'),
                        'convergence': model_data.get('convergence')
                    },
                    'raw_output': {
                        'volatility_stats': model_data.get('volatility_stats', {}),
                        'conditional_volatility_sample': model_data.get('conditional_volatility_sample', [])
                    },
                    'notes': f'GARCH volatility analysis for {model_name}'
                }
                advanced_results.append(result)
        
        # Extract VAR model
        if 'var_model' in data:
            var_data = data['var_model']
            if isinstance(var_data, dict) and 'model' in var_data:
                result = {
                    'test_category': 'Advanced Models',
                    'test_name': 'vector_autoregression_analysis',
                    'execution_date': execution_date,
                    'model_type': 'VAR',
                    'parameters': {
                        'selected_lags': var_data.get('model_diagnostics', {}).get('selected_lags'),
                        'optimal_lags_aic': var_data.get('model_diagnostics', {}).get('optimal_lags_aic'),
                        'n_variables': var_data.get('model_diagnostics', {}).get('n_variables')
                    },
                    'performance_metrics': var_data.get('model_diagnostics', {}).get('model_fit', {}),
                    'raw_output': {
                        'causality_tests': var_data.get('causality_tests', {}),
                        'variables': var_data.get('variables', []),
                        'data_period': var_data.get('data_period', {})
                    },
                    'notes': 'Vector Autoregression analysis with causality testing'
                }
                advanced_results.append(result)
        
        # Extract correlation strategies (advanced trading analysis)
        correlation_strategies = [k for k in data.keys() if k.endswith('_correlation_strategy')]
        for strategy_name in correlation_strategies:
            strategy_data = data[strategy_name]
            if isinstance(strategy_data, dict):
                result = {
                    'test_category': 'Trading Strategies',
                    'test_name': f'{strategy_name}_analysis',
                    'execution_date': execution_date,
                    'model_type': 'Correlation Strategy',
                    'parameters': {
                        'correlation_mean': strategy_data.get('parameters', {}).get('correlation_mean'),
                        'correlation_std': strategy_data.get('parameters', {}).get('correlation_std'),
                        'threshold': strategy_data.get('parameters', {}).get('threshold'),
                        'window': strategy_data.get('parameters', {}).get('window')
                    },
                    'performance_metrics': {
                        'avg_correlation': strategy_data.get('correlation_stats', {}).get('avg_correlation'),
                        'max_correlation': strategy_data.get('correlation_stats', {}).get('max_correlation'),
                        'min_correlation': strategy_data.get('correlation_stats', {}).get('min_correlation'),
                        'total_signals': strategy_data.get('signals', {}).get('total_signals'),
                        'signal_frequency': strategy_data.get('signals', {}).get('signal_frequency')
                    },
                    'raw_output': {
                        'signals': strategy_data.get('signals', {}),
                        'strategy': strategy_data.get('strategy', {}),
                        'z_scores_sample': strategy_data.get('z_scores_sample', [])
                    },
                    'notes': f'Correlation-based trading strategy analysis for {strategy_name}'
                }
                advanced_results.append(result)
        
        logger.info(f"Extracted {len(advanced_results)} advanced results from {json_file_path}")
        return advanced_results
        
    except Exception as e:
        logger.error(f"Failed to extract advanced results from {json_file_path}: {str(e)}")
        return []

def save_advanced_results_to_db(advanced_results: list) -> bool:
    """Save advanced results to diamond_advanced_results table"""
    try:
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
        # Clear existing data
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM diamond_advanced_results"))
            conn.commit()
            logger.info("Cleared existing data from diamond_advanced_results")
        
        # Insert new data
        with engine.connect() as conn:
            for result in advanced_results:
                insert_sql = """
                INSERT INTO diamond_advanced_results 
                (test_category, test_name, execution_date, model_type, parameters, 
                 performance_metrics, raw_output, notes)
                VALUES (:test_category, :test_name, :execution_date, :model_type, 
                        :parameters, :performance_metrics, :raw_output, :notes)
                """
                
                conn.execute(text(insert_sql), {
                    'test_category': result['test_category'],
                    'test_name': result['test_name'],
                    'execution_date': result['execution_date'],
                    'model_type': result['model_type'],
                    'parameters': json.dumps(result['parameters']),
                    'performance_metrics': json.dumps(result['performance_metrics']),
                    'raw_output': json.dumps(result['raw_output']),
                    'notes': result['notes']
                })
            
            conn.commit()
            logger.info(f"Successfully inserted {len(advanced_results)} advanced results")
            return True
            
    except Exception as e:
        logger.error(f"Failed to save advanced results to database: {str(e)}")
        return False

def main():
    """Main function to populate diamond_advanced_results table"""
    logger.info("Starting diamond_advanced_results population...")
    
    # Create table if needed
    if not create_diamond_advanced_results_table():
        logger.error("Failed to create table")
        return False
    
    # Find the latest raw results file
    data_dir = Path('data/diamond')
    json_files = list(data_dir.glob('raw_results_*.json'))
    
    if not json_files:
        logger.error("No raw results JSON files found")
        return False
    
    # Use the latest file
    latest_file = max(json_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Processing file: {latest_file}")
    
    # Extract advanced results
    advanced_results = extract_advanced_results(str(latest_file))
    
    if not advanced_results:
        logger.error("No advanced results found in the JSON file")
        return False
    
    # Save to database
    success = save_advanced_results_to_db(advanced_results)
    
    if success:
        logger.info("Successfully populated diamond_advanced_results table!")
        
        # Verify the data
        engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        with engine.connect() as conn:
            count_result = conn.execute(text("SELECT COUNT(*) FROM diamond_advanced_results"))
            count = count_result.fetchone()[0]
            logger.info(f"Total records in diamond_advanced_results: {count}")
            
            # Show sample data
            sample_result = conn.execute(text("""
                SELECT test_category, test_name, model_type, execution_date 
                FROM diamond_advanced_results 
                LIMIT 5
            """))
            logger.info("Sample records:")
            for row in sample_result.fetchall():
                logger.info(f"  {row[0]} | {row[1]} | {row[2]} | {row[3]}")
    else:
        logger.error("Failed to populate diamond_advanced_results table")
    
    return success

if __name__ == "__main__":
    main() 