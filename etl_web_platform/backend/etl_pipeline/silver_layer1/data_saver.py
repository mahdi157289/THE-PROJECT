from datetime import datetime
import os
import tempfile
import shutil
import time
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging
import math
from typing import Tuple

# Configuration for zero value checks
ZERO_CHECK_CONFIG = {
    'silver_cotations': ['ouverture', 'cloture', 'plus_bas', 'plus_haut', 'capitaux'],
    'silver_indices': ['indice_jour', 'indice_veille', 'indice_plus_haut', 'indice_plus_bas'],
    'rejected_prime': []  # No zero checks for rejected records
}

class DataSaver:
    def __init__(self, engine, logger: logging.Logger):
        self.engine = engine
        self.logger = logger
        self.MAX_PARAMETERS_PER_QUERY = 100000  # PostgreSQL's parameter limit

    def save_to_parquet(self, df: pd.DataFrame, path: str, max_retries: int = 5) -> None:
        """Save DataFrame to Parquet with atomic write"""
        temp_path = None
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            for attempt in range(1, max_retries + 1):
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
                        temp_path = tmp_file.name
                    
                    df.to_parquet(temp_path, index=False)
                    
                    try:
                        if os.path.exists(path):
                            os.chmod(path, 0o777)
                            os.remove(path)
                    except PermissionError:
                        self.logger.warning(f"Could not remove existing file (attempt {attempt})")
                    
                    shutil.move(temp_path, path)
                    self.logger.info(f"Saved {len(df):,} rows to {path}")
                    return
                    
                except PermissionError:
                    self.logger.warning(f"Permission denied (attempt {attempt})")
                    if temp_path and os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
                    
                    if attempt < max_retries:
                        time.sleep(2 ** attempt)
                    else:
                        self.logger.warning("Attempting direct write as fallback")
                        df.to_parquet(path, index=False)
                        return
                        
        except Exception as e:
            self.logger.error(f"Failed to save Parquet file: {str(e)}")
            raise

    def _filter_zero_values(self, df: pd.DataFrame, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filter out records with zero values in critical columns.
        Returns: (clean_df, rejected_df)
        """
        # Get columns to check for this table
        zero_cols = next(
            (cols for key, cols in ZERO_CHECK_CONFIG.items() if key in table_name),
            []
        )
        
        if not zero_cols or df.empty:
            return df, pd.DataFrame()

        # Create mask for rows with any zeros in critical columns
        zero_mask = (df[zero_cols] == 0).any(axis=1)
        
        if not zero_mask.any():
            return df, pd.DataFrame()

        # Prepare rejected records
        rejected_df = df[zero_mask].copy()
        rejected_df['rejection_reason'] = 'ZERO_VALUE_IN_' + rejected_df[zero_cols].eq(0).idxmax(axis=1)
        rejected_df['zero_value_columns'] = rejected_df[zero_cols].apply(
            lambda row: ','.join(row.index[row == 0]),
            axis=1
        )
        rejected_df['rejection_timestamp'] = datetime.now()
        rejected_df['source_file'] = table_name
        
        # Return clean data and rejects
        return df[~zero_mask], rejected_df

    def _log_and_save_rejects(self, rejected_df: pd.DataFrame, source_table: str) -> None:
        """Centralized handling of rejected records"""
        if not rejected_df.empty:
            # Get the list of columns that exist in the rejected_prime table
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'rejected_prime'"
                ))
                existing_columns = {row[0] for row in result}
            
            # Filter DataFrame to only include existing columns
            columns_to_keep = [col for col in rejected_df.columns if col in existing_columns]
            filtered_df = rejected_df[columns_to_keep].copy()
            
            # Sample and save (keep first 10 for debugging)
            sample_size = min(10, len(filtered_df))
            self.logger.warning(
                f"Rejected {len(filtered_df)} records. "
                f"Saving sample of {sample_size} to rejected_prime."
            )
            
            # Save to rejected_prime
            self.save_to_postgres(
                filtered_df.head(sample_size),
                'rejected_prime',
                schema_name='public'
            )

    def _get_postgres_dtype_mapping(self, df: pd.DataFrame) -> dict:
        """Create mapping of pandas dtypes to PostgreSQL types"""
        dtype_mapping = {}
        for column, dtype in df.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                dtype_mapping[column] = 'BIGINT'
            elif pd.api.types.is_float_dtype(dtype):
                dtype_mapping[column] = 'DOUBLE PRECISION'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                dtype_mapping[column] = 'TIMESTAMP'
            elif pd.api.types.is_bool_dtype(dtype):
                dtype_mapping[column] = 'BOOLEAN'
            else:
                dtype_mapping[column] = 'TEXT'
        
        return dtype_mapping
    
    def _create_table_if_not_exists(self, df: pd.DataFrame, table_name: str, schema_name: str = None):
        """Create table dynamically based on DataFrame structure"""
        try:
            # Parse schema and table name
            if '.' in table_name:
                schema_name, table_name_only = table_name.split('.', 1)
            else:
                schema_name = schema_name if schema_name else 'public'
                table_name_only = table_name

            # Check if schema exists, create if not
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT schema_name FROM information_schema.schemata 
                    WHERE schema_name = '{schema_name}'
                """))
                if not result.fetchone():
                    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
                    conn.commit()
                    self.logger.info(f"Created schema: {schema_name}")

            # Check if table exists
            inspector = inspect(self.engine)
            if not inspector.has_table(table_name_only, schema=schema_name):
                # Get column types
                dtype_mapping = self._get_postgres_dtype_mapping(df)
                
                # Build CREATE TABLE statement
                columns = []
                for col_name, col_type in dtype_mapping.items():
                    columns.append(f'"{col_name}" {col_type}')
                
                create_table_sql = f"""
                CREATE TABLE "{schema_name}"."{table_name_only}" (
                    {', '.join(columns)}
                )
                """
                
                with self.engine.connect() as conn:
                    conn.execute(text(create_table_sql))
                    conn.commit()
                
                self.logger.info(f"Created table: {schema_name}.{table_name_only}")
            else:
                self.logger.debug(f"Table exists: {schema_name}.{table_name_only}")
                
        except Exception as e:
            self.logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    def execute_ddl(self, ddl_statement: str) -> None:
        """Execute raw DDL statements"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(ddl_statement))
                conn.commit()
            self.logger.info(f"Executed DDL statement successfully")
        except Exception as e:
            self.logger.error(f"Failed to execute DDL: {str(e)}")
            raise

    def save_to_postgres(self, df: pd.DataFrame, table_name: str, schema_name: str = None) -> None:
        """Save DataFrame to PostgreSQL with zero value filtering"""
        max_retries = 3
        total_rows = len(df)
        
        # Step 1: Initial NULL filtering
        df = df.dropna(how="any")
        
        # Step 2: Filter zero values
        clean_df, zero_rejects = self._filter_zero_values(df, table_name)
        
        # Step 3: Save sample of zero-rejected records
        if not zero_rejects.empty:
            self._log_and_save_rejects(zero_rejects, table_name)
        
        # Proceed only if we have clean data
        if clean_df.empty:
            self.logger.warning(f"No valid records to save after filtering for {table_name}")
            return

        # Step 4: Verify no zeros remain
        zero_cols = ZERO_CHECK_CONFIG.get(table_name, [])
        if zero_cols and (clean_df[zero_cols] == 0).any().any():
            self.logger.error("Zero values detected after filtering! Aborting save.")
            return

        # Calculate safe chunk size
        num_columns = len(clean_df.columns)
        safe_chunk_size = max(1, self.MAX_PARAMETERS_PER_QUERY // num_columns // 2)
        chunks = math.ceil(total_rows / safe_chunk_size)
        
        self.logger.info(f"📊 Starting PostgreSQL save to '{table_name}'")
        self.logger.debug(f"  • Total rows: {total_rows:,}")
        self.logger.debug(f"  • Clean rows after filtering: {len(clean_df):,}")
        self.logger.debug(f"  • Columns: {num_columns}")
        self.logger.debug(f"  • Calculated chunk size: {safe_chunk_size}")
        self.logger.debug(f"  • Total chunks: {chunks}")

        # Create table dynamically if it doesn't exist
        try:
            self._create_table_if_not_exists(clean_df, table_name, schema_name)
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {str(e)}")
            raise

        # Data insertion logic
        for chunk_idx, start in enumerate(range(0, len(clean_df), safe_chunk_size)):
            end = min(start + safe_chunk_size, len(clean_df))
            chunk_df = clean_df.iloc[start:end]
            chunk_size = len(chunk_df)
            
            for attempt in range(max_retries):
                try:
                    self.logger.debug(
                        f"💾 Saving chunk {chunk_idx+1}/{chunks} "
                        f"(rows {start+1}-{end}, size: {chunk_size}) "
                        f"(attempt {attempt+1}/{max_retries})"
                    )
                    
                    chunk_df.to_sql(
                        table_name.split('.')[-1] if '.' in table_name else table_name,
                        self.engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=chunk_size,
                        schema=table_name.split('.')[0] if '.' in table_name else schema_name
                    )
                    
                    self.logger.debug(f"✅ Saved chunk {chunk_idx+1}/{chunks} successfully")
                    break
                    
                except SQLAlchemyError as e:
                    error_type = type(e).__name__
                    error_msg = str(e)
                    
                    self.logger.error(f"💥 Database error during chunk save (attempt {attempt+1})")
                    self.logger.debug(f"Error type: {error_type}")
                    self.logger.debug(f"Full error message: {error_msg}")
                    
                    if chunk_size > 0:
                        problem_sample = chunk_df.head(1).to_dict(orient='records')[0]
                        self.logger.debug(f"Problematic data sample: {problem_sample}")
                    
                    if attempt == max_retries - 1:
                        self.logger.critical("💢 Final save attempt failed")
                        raise
                    
                    sleep_time = (2 ** attempt) + (0.1 * attempt)
                    self.logger.warning(f"⏳ Retrying in {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    
                except Exception as e:
                    self.logger.critical("💢 Unexpected error during chunk save")
                    self.logger.error(f"Error type: {type(e).__name__}")
                    self.logger.error(f"Error message: {str(e)}")
                    self.logger.debug(f"Chunk details: rows {start+1}-{end}, size {chunk_size}")
                    raise

        self.logger.info(f"🏁 Successfully saved {len(clean_df):,} rows to '{table_name}'")