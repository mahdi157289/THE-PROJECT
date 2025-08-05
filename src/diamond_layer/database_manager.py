# src/diamond_layer/database_manager.py
import logging
from sqlalchemy import text, inspect
from typing import List, Dict
import pandas as pd
from datetime import datetime

class DiamondDatabaseManager:
    
    def __init__(self, engine, logger=None):
        self.engine = engine
        self.logger = logger or logging.getLogger(__name__)
        self._configure_logger() 
        
    def _table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """Check if table exists"""
        return inspect(self.engine).has_table(table_name, schema=schema)
    
    def _get_table_columns(self, table_name: str, schema: str = 'public') -> Dict[str, str]:
        """Get current column definitions"""
        inspector = inspect(self.engine)
        return {col['name']: str(col['type']) for col in inspector.get_columns(table_name, schema=schema)}
    
    def execute_ddl(self, ddl_statements: List[str], force_recreate: bool = False):
        """Execute DDL with safeguards"""
        with self.engine.connect() as conn:
            for stmt in ddl_statements:
                try:
                    if 'CREATE TABLE' in stmt.upper():
                        table_name = self._extract_table_name(stmt)
                        if self._table_exists(table_name):
                            if force_recreate:
                                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                                self.logger.info(f"Dropped existing table {table_name}")
                            else:
                                self._alter_existing_table(conn, table_name, stmt)
                                continue
                    
                    conn.execute(text(stmt))
                    conn.commit()
                    self.logger.info(f"Executed DDL: {stmt[:100]}...")
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"DDL execution failed: {str(e)}")
                    raise
    
    def save_metrics(self, metrics_data: List[Dict]) -> bool:
        """
        Save metrics to diamond_metrics table using batch inserts
        
        Args:
            metrics_data: List of dictionaries containing metric data
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not metrics_data:
            self.logger.warning("No metrics data to save")
            return False
            
        try:
            # Convert to DataFrame for easier handling
            df = pd.DataFrame(metrics_data)
            
            # Ensure required columns exist
            required_columns = ['test_name', 'category', 'execution_time', 'status']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns: {missing_columns}")
                return False
            
            # Insert data into database using batch processing
            with self.engine.connect() as conn:
                # Use batch inserts to avoid SQL parameter limits
                batch_size = 100  # Process in smaller batches
                total_inserted = 0
                
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size]
                    
                    # Insert batch using pandas to_sql with smaller chunks
                    batch_df.to_sql('diamond_metrics', conn, if_exists='append', 
                                   index=False, method='multi', chunksize=50)
                    
                    total_inserted += len(batch_df)
                    self.logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_df)} records")
                
                conn.commit()
                self.logger.info(f"Successfully saved {total_inserted} metrics to diamond_metrics table")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to save metrics to database: {str(e)}")
            return False
    
    def get_metrics(self, limit: int = 100) -> pd.DataFrame:
        """
        Retrieve metrics from diamond_metrics table
        
        Args:
            limit: Maximum number of records to retrieve
            
        Returns:
            pd.DataFrame: Metrics data
        """
        try:
            query = f"SELECT * FROM diamond_metrics ORDER BY execution_time DESC LIMIT {limit}"
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f"Failed to retrieve metrics: {str(e)}")
            return pd.DataFrame()
    
    def _alter_existing_table(self, conn, table_name: str, create_stmt: str):
        """Handle ALTER TABLE logic"""
        current_columns = self._get_table_columns(table_name)
        new_columns = self._parse_columns_from_ddl(create_stmt)
        
        for col_name, col_type in new_columns.items():
            if col_name not in current_columns:
                alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                conn.execute(text(alter_stmt))
                self.logger.info(f"Added column {col_name} to {table_name}")
            elif current_columns[col_name] != col_type:
                self.logger.warning(
                    f"Type mismatch for {col_name}: "
                    f"existing {current_columns[col_name]} vs new {col_type}"
                )
    
    def _extract_table_name(self, ddl: str) -> str:
        """Parse table name from CREATE statement"""
        return ddl.split()[2].split('(')[0].strip()
    
    def _parse_columns_from_ddl(self, ddl: str) -> Dict[str, str]:
        """Extract column definitions from DDL"""
        cols_part = ddl.split('(')[1].rsplit(')', 1)[0]
        columns = {}
        for line in cols_part.split(','):
            line = line.strip()
            if line and not line.upper().startswith(('CONSTRAINT', 'PRIMARY KEY')):
                col_parts = line.split()
                columns[col_parts[0]] = ' '.join(col_parts[1:])
        return columns
    
    def _configure_logger(self):
        """Ensure logger has phase field"""
        if not hasattr(self.logger, 'phase'):
            self.logger.phase = 'DATABASE'
        if not self.logger.handlers:
            # Add basic console handler if none exists
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)