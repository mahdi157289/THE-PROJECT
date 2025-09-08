"""
Extended saver for Diamond Layer results
"""

from src.golden_layer.golden_saver import GoldenDataSaver
from typing import Dict, Any
import pandas as pd
import dask.dataframe as dd
import logging

from .database_manager import DiamondDatabaseManager

class DiamondDataSaver(GoldenDataSaver):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics_table = 'diamond_metrics'
        self.results_table = 'diamond_results'
        self.anomalies_table = 'diamond_anomalies'
        self.db_manager = DiamondDatabaseManager(self.engine, self.logger)

    def initialize_schema(self):
        """Execute all DDL with safeguards"""
        ddl_statements = [
            self._get_metrics_schema(),
            self._get_results_schema(),
            self._get_anomalies_schema()
        ]
        self.db_manager.execute_ddl(ddl_statements)

    def _create_table_safe(self, table_name: str, schema: str):
        """Wrapper for safe table creation"""
        self.db_manager.execute_ddl([schema])
        
    def _get_metrics_schema(self) -> str:
        """Consolidated metrics schema"""
        return """
        CREATE TABLE IF NOT EXISTS diamond_metrics (
            metric_id SERIAL PRIMARY KEY,
            test_name VARCHAR(255) NOT NULL,
            test_type VARCHAR(100),
            execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            statistic_value DOUBLE PRECISION,
            p_value DOUBLE PRECISION,
            test_duration INTERVAL,
            details JSONB,
            CONSTRAINT valid_status CHECK (status IN ('success', 'failed', 'warning'))
        )
        """
    def _get_results_schema(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS diamond_results (
            result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            metric_id INTEGER REFERENCES diamond_metrics(metric_id),
            asset_type VARCHAR(50) NOT NULL,
            validation_date DATE NOT NULL,
            raw_results JSONB NOT NULL
        )
        """
    
    def _get_anomalies_schema(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS diamond_anomalies (
            anomaly_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            metric_id INTEGER REFERENCES diamond_metrics(metric_id),
            detected_at TIMESTAMP NOT NULL,
            severity VARCHAR(20) NOT NULL,
            description TEXT,
            resolution_status VARCHAR(20) DEFAULT 'unresolved',
            CONSTRAINT valid_severity CHECK (severity IN ('low', 'medium', 'high')),
            CONSTRAINT valid_resolution CHECK (resolution_status IN ('unresolved', 'investigating', 'resolved'))
        )
        """
    
    def _get_metrics_schema(self) -> str:
        """Schema for validation results table"""
        return """
        CREATE TABLE IF NOT EXISTS diamond_validation_metrics (
            metric_id SERIAL PRIMARY KEY,
            test_name VARCHAR(255) NOT NULL,
            test_type VARCHAR(100) NOT NULL,
            execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(50) NOT NULL,
            statistic_value DOUBLE PRECISION,
            p_value DOUBLE PRECISION,
            details JSONB
        )
        """
    
    def save_metrics(self, metrics: Dict[str, Any]):
        """Save validation metrics to database"""
        try:
            # Convert metrics to DataFrame
            metrics_df = self._prepare_metrics(metrics)
            
            # Create table if needed
            self._create_table_safe(self.metrics_table, self._get_metrics_schema())
            
            # Save to database
            metrics_df.to_sql(
                self.metrics_table,
                self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"Saved {len(metrics_df)} metrics to {self.metrics_table}",
                           extra={'phase': 'METRICS_SAVE'})
        except Exception as e:
            self.logger.error(f"Failed to save metrics: {str(e)}",
                            extra={'phase': 'METRICS_SAVE'})
            raise
    
    def _prepare_metrics(self, metrics: Dict[str, Any]) -> pd.DataFrame:
        """Convert metrics dictionary to DataFrame"""
        rows = []
        for test_name, result in metrics.items():
            row = {
                'test_name': test_name,
                'test_type': 'basic' if 'error' not in test_name else 'error',
                'status': 'completed' if not isinstance(result, dict) or 'error' not in result else 'failed'
            }
            
            if isinstance(result, dict):
                if 'statistic' in result:
                    row['statistic_value'] = result['statistic']
                if 'p_value' in result:
                    row['p_value'] = result['p_value']
                row['details'] = str(result)
            else:
                row['details'] = str(result)
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def save_advanced_metrics(self, metrics: Dict[str, Any]):
        """Save advanced validation metrics"""
        try:
            # Convert to DataFrame
            metrics_df = pd.DataFrame([{
                'test_name': k,
                'test_type': 'advanced',
                'details': str(v)
            } for k, v in metrics.items()])
            
            # Save to database
            metrics_df.to_sql(
                'diamond_advanced_metrics',
                self.engine,
                if_exists='append',
                index=False
            )
            self.logger.info(f"Saved {len(metrics_df)} advanced metrics", 
                           extra={'phase': 'ADVANCED_SAVE'})
        except Exception as e:
            self.logger.error(f"Failed to save advanced metrics: {str(e)}")