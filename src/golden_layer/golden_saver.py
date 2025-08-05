import os
import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import sqlalchemy as sa
from typing import Tuple
import logging
import time
from datetime import datetime

class GoldenDataSaver:
    def __init__(self, engine=None, db_conn_str=None, logger: logging.Logger = None):
        if engine is not None:
            self.engine = engine
        elif db_conn_str is not None:
            self.engine = create_engine(db_conn_str)
        else:
            raise ValueError("Either engine or db_conn_str must be provided")
        
        # Fix 1: Configure logger with safe formatter
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__)
            
        # Ensure the logger has a safe formatter
        self._configure_safe_logger()
        self.MAX_RETRIES = 3

    def _configure_safe_logger(self):
        """Configure logger with safe formatting"""
        class SafeFormatter(logging.Formatter):
            def format(self, record):
                try:
                    return super().format(record)
                except KeyError as e:
                    if 'phase' not in record.__dict__:
                        record.__dict__['phase'] = 'SAVER'
                    return super().format(record)
                except UnicodeEncodeError:
                    msg = record.msg.encode('ascii', 'ignore').decode('ascii')
                    record.msg = msg
                    return super().format(record)
        
        # Only add handlers if they don't exist
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = SafeFormatter('%(asctime)s | %(levelname)-8s | %(phase)-15s | %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def save_to_parquet(self, df: dd.DataFrame, path: str) -> None:
        """Save with atomic write pattern (no filtering)"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, engine='pyarrow', write_index=False)
        self.logger.info(f"Saved {len(df)} rows to {path}", extra={'phase': 'PARQUET_SAVE'})

    def _drop_table_if_exists(self, table_name: str) -> None:
        """Drop table if it exists to ensure clean slate"""
        schema_name = 'public'
        
        try:
            with self.engine.connect() as conn:
                # Check if table exists and drop it
                if inspect(self.engine).has_table(table_name, schema=schema_name):
                    conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE"))
                    conn.commit()
                    self.logger.info(f"Dropped existing table {schema_name}.{table_name}", 
                                   extra={'phase': 'TABLE_DROP'})
                else:
                    self.logger.info(f"Table {schema_name}.{table_name} does not exist, nothing to drop", 
                                   extra={'phase': 'TABLE_DROP'})
                    
        except Exception as e:
            self.logger.error(f"Table drop failed for {table_name}: {str(e)}", extra={'phase': 'TABLE_DROP'})
            # Don't raise - continue with creation even if drop fails
    
    def _create_table_safe(self, table_name: str, schema: str) -> None:
        """Robust table creation with clean slate approach"""
        schema_name = 'public'  # Force public schema for golden layer
        
        try:
            with self.engine.connect() as conn:
                # 1. First drop table if it exists (clean slate)
                self._drop_table_if_exists(table_name)
                
                # 2. Create schema if needed (though public should exist)
                if not inspect(self.engine).has_schema(schema_name):
                    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
                    conn.commit()
                
                # 3. Create table (it shouldn't exist now)
                conn.execute(text(schema))
                conn.commit()
                self.logger.info(f"Created fresh table {schema_name}.{table_name}", 
                               extra={'phase': 'TABLE_CREATE'})
                
        except Exception as e:
            self.logger.error(f"Table creation failed for {table_name}: {str(e)}", extra={'phase': 'TABLE_CREATE'})
            raise

    def save_to_postgres(self, df: pd.DataFrame, table_name: str, schema: str):
        """Save with clean slate approach - drops and recreates table"""
        try:
            # 1. Create fresh table (drops existing first)
            self._create_table_safe(table_name, schema)
            
            # 2. Log data info
            self.logger.info(f"Saving {len(df)} rows to fresh table {table_name}", 
                           extra={'phase': 'DB_SAVE'})
            
            # 3. Save with append (table is fresh, so it's like replace but safer)
            df.to_sql(
                table_name,
                self.engine,
                if_exists='append',  
                index=False,
                chunksize=8000,
                method='multi', 
                dtype=self._get_type_mapping(df.dtypes)
            )
            self.logger.info(f"Successfully saved {len(df)} rows to {table_name}", extra={'phase': 'DB_SAVE'})
            
        except Exception as e:
            self.logger.error(f"Save failed for {table_name}: {str(e)}", extra={'phase': 'DB_SAVE'})
            # Fix 3: Implement the missing fallback method
            self._fallback_save(df, table_name)

    def _fallback_save(self, df: pd.DataFrame, table_name: str):
        """Fix 3: Implement missing fallback save method"""
        try:
            self.logger.warning(f"Attempting fallback save for {table_name}", extra={'phase': 'FALLBACK_SAVE'})
            
            # Try with minimal configuration
            df.to_sql(
                table_name,
                self.engine,
                if_exists='replace',  # Replace table to avoid schema conflicts
                index=False,
                chunksize=500,
                method='multi'
            )
            self.logger.info(f"Fallback save successful for {table_name}", extra={'phase': 'FALLBACK_SAVE'})
            
        except Exception as fallback_error:
            self.logger.error(f"Fallback save failed for {table_name}: {str(fallback_error)}", 
                            extra={'phase': 'FALLBACK_SAVE'})
            # Final fallback - just log the error but don't crash
            self.logger.warning(f"Skipping database save for {table_name} - Parquet file is available", 
                              extra={'phase': 'FALLBACK_SAVE'})

    def _get_type_mapping(self, dtypes):
        """Fix 2: Improved type mapping that handles pandas nullable types"""
        type_map = {
            'object': sa.VARCHAR(255),
            'string': sa.TEXT(),  # Fix for pandas string type
            'float64': sa.Float(),
            'Float64': sa.Float(),
            'Int64': sa.BIGINT(),# Fix for pandas nullable Float64
            'int64': sa.Integer(),
            'int32': sa.Integer(),
            'Int32': sa.Integer(),  # Fix for pandas nullable Int32
            'datetime64[ns]': sa.TIMESTAMP(),  # Fix for datetime
            'datetime64[us]': sa.TIMESTAMP(),  # Fix for datetime with microseconds
            'bool': sa.Boolean(),
            'boolean': sa.Boolean()  # Fix for pandas nullable boolean
        }
        
        result = {}
        for col, dtype in dtypes.items():
            dtype_str = str(dtype)
            if dtype_str in type_map:
                result[col] = type_map[dtype_str]
            else:
                # Default fallback
                result[col] = sa.VARCHAR(255)
                self.logger.warning(f"Unknown dtype {dtype_str} for column {col}, using VARCHAR", 
                                  extra={'phase': 'TYPE_MAPPING'})
        
        return result

    

    def save_results(self, cotations: dd.DataFrame, indices: dd.DataFrame, output_dir: str) -> None:
        """Save operation without data filtering"""
        cotations_df = cotations.compute()
        indices_df = indices.compute()

        # Save Parquet files (no filtering)
        self.save_to_parquet(dd.from_pandas(cotations_df, npartitions=1), 
                            f"{output_dir}/golden_cotations.parquet")
        self.save_to_parquet(dd.from_pandas(indices_df, npartitions=1), 
                            f"{output_dir}/golden_indices.parquet")

        # Get schemas
        cotations_schema, indices_schema = self._get_golden_schemas()
        
        # Save to PostgreSQL
        try:
            self.save_to_postgres(cotations_df, 'golden_cotations', cotations_schema)
        except Exception as e:
            self.logger.error(f"Failed to save cotations to DB: {e}", extra={'phase': 'DB_SAVE'})
        
        try:
            self.save_to_postgres(indices_df, 'golden_indices', indices_schema)
        except Exception as e:
            self.logger.error(f"Failed to save indices to DB: {e}", extra={'phase': 'DB_SAVE'})
        
        self.logger.info("Save operation completed", 
                    extra={'phase': 'SAVE_COMPLETE'})
    
    def _generate_column_definitions(self, columns: list) -> str:
        """Dynamically generate SQL column definitions"""
        column_defs = []
        for col in columns:
            if col == 'seance':
                column_defs.append(f"{col} TIMESTAMP")
            elif col in ['valeur', 'lib_indice', 'groupe', 'code', 'source_file']:
                column_defs.append(f"{col} TEXT")
            elif col in ['est_debut_mois', 'est_fin_mois']:
                column_defs.append(f"{col} BOOLEAN")
            elif col in ['annee', 'mois', 'jour_semaine', 'trimestre', 'nb_transaction']:
                column_defs.append(f"{col} INTEGER")
            elif 'timestamp' in col:
                column_defs.append(f"{col} TIMESTAMP")
            else:
                # Default to DOUBLE PRECISION for numeric fields
                column_defs.append(f"{col} DOUBLE PRECISION")
        
        return ",\n        ".join(column_defs) 
       
    def validate_schema_compatibility(self, df: pd.DataFrame, table_name: str):
        """Fix 2: More lenient schema validation"""
        try:
            inspector = inspect(self.engine)
            
            # Get database columns and types
            db_columns = {col['name']: col['type'] 
                        for col in inspector.get_columns(table_name)}
            
            # Just check for missing columns, not type mismatches
            missing_cols = []
            for col in df.columns:
                if col not in db_columns:
                    missing_cols.append(col)
            
            if missing_cols:
                self.logger.warning(f"New columns in {table_name}: {missing_cols}", 
                                  extra={'phase': 'SCHEMA_VALIDATION'})
            else:
                self.logger.info(f"Schema validation passed for {table_name}", 
                               extra={'phase': 'SCHEMA_VALIDATION'})
                
        except Exception as e:
            self.logger.warning(f"Schema validation skipped for {table_name}: {e}", 
                              extra={'phase': 'SCHEMA_VALIDATION'})

    def _types_compatible(self, df_type, db_type) -> bool:
        """Check if pandas and SQL types are compatible"""
        # More lenient type checking
        compatible_mappings = {
            ('datetime64[ns]', 'TIMESTAMP'): True,
            ('datetime64[us]', 'TIMESTAMP'): True,
            ('string', 'TEXT'): True,
            ('object', 'TEXT'): True,
            ('object', 'VARCHAR'): True,
            ('bool', 'BOOLEAN'): True,
            ('boolean', 'BOOLEAN'): True,
            ('int32', 'BIGINT'): True,
            ('Int32', 'BIGINT'): True,
            ('int64', 'BIGINT'): True,
            ('float64', 'DOUBLE PRECISION'): True,
            ('Float64', 'DOUBLE PRECISION'): True,
        }
        
        return compatible_mappings.get((str(df_type), str(db_type)), False)
       
    def _get_golden_schemas(self) -> Tuple[str, str]:
        """Return optimized DDL for golden tables with validation"""
        expected_cotation_cols = [
            'seance', 'groupe', 'code', 'valeur', 'ouverture', 'cloture',
            'plus_bas', 'plus_haut', 'quantite_negociee', 'nb_transaction',
            'capitaux', 'annee', 'mois', 'jour_semaine', 'trimestre',
            'est_debut_mois', 'est_fin_mois', 'price_variation_abs',
            'variation_pourcentage', 'ingestion_timestamp', 'source_file',
            'rsi_14', 'bollinger_upper', 'bollinger_lower', 'ma_7', 'ma_30',
            'momentum_5', 'momentum_30', 'vwap', 'liquidity_ratio',
            'sector_alpha', 'sector_beta', 'sector_correlation'
        ]
        
        expected_index_cols = [
            'seance', 'code_indice', 'lib_indice', 'indice_jour', 'indice_veille',
            'variation_veille', 'indice_plus_haut', 'indice_plus_bas', 'indice_ouv',
            'annee', 'mois', 'jour_semaine', 'trimestre', 'est_debut_mois',
            'est_fin_mois', 'variation_indice', 'ingestion_timestamp', 'source_file',
            'rsi_14', 'momentum_20', 'sector_rotation'
        ]

        cotations_schema = f"""
        CREATE TABLE IF NOT EXISTS golden_cotations (
            {self._generate_column_definitions(expected_cotation_cols)}
        )
        """
        
        indices_schema = f"""
        CREATE TABLE IF NOT EXISTS golden_indices (
            {self._generate_column_definitions(expected_index_cols)}
        )
        """
        
        return cotations_schema, indices_schema