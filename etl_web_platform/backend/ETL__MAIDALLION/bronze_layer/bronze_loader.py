import os
import shutil
import tempfile
import time
import logging
from datetime import datetime
import pandas as pd
import json
from pathlib import Path
import numpy as np
from timeit import default_timer as timer
from functools import wraps
from sqlalchemy import create_engine

# Import your existing configs
import sys
import os

# Add the ETL directory to Python path for local imports
etl_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if etl_path not in sys.path:
    sys.path.insert(0, etl_path)

# Add the etl_pipeline directory to Python path
etl_pipeline_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'etl_pipeline'))
if etl_pipeline_path not in sys.path:
    sys.path.insert(0, etl_pipeline_path)

# Use existing config from etl_pipeline
from config.settings import PATHS
from utils.db_connection import DatabaseConnector
from schemas import create_bronze_schemas

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles numpy data types"""
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                        np.int16, np.int32, np.int64, np.uint8,
                        np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

def timed_operation(operation_name=None):
    """Decorator to log operation timing"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            op_name = operation_name if operation_name else func.__name__.replace('_', ' ').title()
            op_logger = logging.getLogger(f'BronzeLevelLoader.operations.{op_name.replace(" ", "_").lower()}')
            op_logger.info(f"🚀 Starting operation: {op_name}")
            start_time = timer()
            try:
                result = func(*args, **kwargs)
                duration = timer() - start_time
                op_logger.info(f"✅ Completed {op_name} in {duration:.3f} seconds")
                return result
            except Exception as e:
                duration = timer() - start_time
                op_logger.error(f"❌ Failed {op_name} after {duration:.3f} seconds", exc_info=True)
                raise
        return wrapper
    return decorator

class BronzeLevelLoader:
    def __init__(self):
        self.logger = self._setup_logger()
        self.db_engine = self._init_db_engine()
        self.cotations_schema, self.indices_schema = create_bronze_schemas()
        self.logger.info("🏁 BronzeLevelLoader initialized (Pandas-only)")

    def _setup_logger(self):
        """Identical to your original logging setup"""
        logger = logging.getLogger('BronzeLevelLoader')
        logger.setLevel(logging.DEBUG)
        if logger.hasHandlers():
            logger.handlers.clear()
        
        log_file = os.path.join(PATHS['logs'], 'bronze_loader.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s'
        )
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s'
        )
        
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def _init_db_engine(self):
        """Initialize SQLAlchemy engine using your existing DB config"""
        db_config = DatabaseConnector().db_config
        return create_engine(
            f"postgresql://{db_config['username']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

    @timed_operation("Column Case Analysis")
    def _analyze_column_case(self, df: pd.DataFrame, data_type: str) -> dict:
        """Identical to your original column analysis"""
        case_logger = logging.getLogger('BronzeLevelLoader.case_analysis')
        case_logger.handlers = self.logger.handlers
        
        try:
            case_logger.info(f"🔍 Analyzing column case patterns for {data_type}")
            columns = df.columns.tolist()
            case_analysis = {
                'data_type': data_type,
                'total_columns': len(columns),
                'columns': columns,
                'case_patterns': {
                    'all_uppercase': [],
                    'all_lowercase': [],
                    'mixed_case': [],
                    'title_case': [],
                    'camel_case': [],
                    'snake_case': [],
                    'other': []
                },
                'statistics': {
                    'uppercase_count': 0,
                    'lowercase_count': 0,
                    'mixed_case_count': 0,
                    'title_case_count': 0,
                    'camel_case_count': 0,
                    'snake_case_count': 0,
                    'other_count': 0
                },
                'dominant_pattern': '',
                'needs_standardization': False
            }
            
            for col_name in columns:
                if not col_name or col_name.isspace():
                    case_analysis['case_patterns']['other'].append(col_name)
                    case_analysis['statistics']['other_count'] += 1
                    continue
                
                if col_name.isupper():
                    case_analysis['case_patterns']['all_uppercase'].append(col_name)
                    case_analysis['statistics']['uppercase_count'] += 1
                elif col_name.islower():
                    if '_' in col_name:
                        case_analysis['case_patterns']['snake_case'].append(col_name)
                        case_analysis['statistics']['snake_case_count'] += 1
                    else:
                        case_analysis['case_patterns']['all_lowercase'].append(col_name)
                        case_analysis['statistics']['lowercase_count'] += 1
                elif col_name.istitle() and '_' not in col_name and col_name.replace(' ', '').isalpha():
                    case_analysis['case_patterns']['title_case'].append(col_name)
                    case_analysis['statistics']['title_case_count'] += 1
                elif any(c.isupper() for c in col_name[1:]) and any(c.islower() for c in col_name):
                    if col_name[0].islower() and '_' not in col_name:
                        case_analysis['case_patterns']['camel_case'].append(col_name)
                        case_analysis['statistics']['camel_case_count'] += 1
                    else:
                        case_analysis['case_patterns']['mixed_case'].append(col_name)
                        case_analysis['statistics']['mixed_case_count'] += 1
                else:
                    case_analysis['case_patterns']['other'].append(col_name)
                    case_analysis['statistics']['other_count'] += 1
            
            stats = case_analysis['statistics']
            max_count = max(stats.values())
            case_analysis['dominant_pattern'] = 'none' if max_count == 0 else \
                next(pattern.replace('_count', '') for pattern, count in stats.items() if count == max_count)
            
            pattern_counts = [count for count in stats.values() if count > 0]
            case_analysis['needs_standardization'] = len(pattern_counts) > 1
            
            case_logger.info(
                f"📊 Case Analysis Summary - {data_type}:\n"
                f"  • Total columns: {case_analysis['total_columns']}\n"
                f"  • Dominant pattern: {case_analysis['dominant_pattern']}\n"
                f"  • Needs standardization: {'Yes' if case_analysis['needs_standardization'] else 'No'}"
            )
            
            for pattern, cols in case_analysis['case_patterns'].items():
                if cols:
                    case_logger.debug(f"  • {pattern}: {len(cols)} columns - {cols}")
            
            if case_analysis['needs_standardization']:
                case_logger.warning("⚠️ Mixed case patterns detected")
            if case_analysis['statistics']['other_count'] > 0:
                case_logger.warning(f"⚠️ Found {case_analysis['statistics']['other_count']} unusual columns")
            
            return case_analysis
            
        except Exception as e:
            case_logger.error("❌ Column case analysis failed", exc_info=True)
            return {
                'data_type': data_type,
                'total_columns': len(df.columns),
                'columns': df.columns.tolist(),
                'error': str(e),
                'dominant_pattern': 'unknown',
                'needs_standardization': True
            }

    @timed_operation("DataFrame Statistics Logging")
    def _log_dataframe_stats(self, df, name, sample_size=2):
        """Adapted for Pandas (same info as original)"""
        stats_logger = logging.getLogger('BronzeLevelLoader.stats')
        stats_logger.handlers = self.logger.handlers
        row_count = len(df)
        col_count = len(df.columns)
        
        stats_logger.info(
            f"📋 {name} Statistics:\n"
            f"  • Rows: {row_count:,}\n"
            f"  • Columns: {col_count}"
        )
        
        stats_logger.debug(f"📜 Columns: {df.columns.tolist()}")
        stats_logger.debug(f"📊 Data types:\n{df.dtypes}")
        
        if row_count > 0:
            stats_logger.debug(f"🔍 Sample data:\n{df.head(sample_size).to_string()}")
        
        null_counts = df.isna().sum()
        null_cols = null_counts[null_counts > 0]
        if not null_cols.empty:
            stats_logger.warning(f"⚠️ Null values detected:\n{null_cols}")

    @timed_operation("Data Preprocessing")
    def _preprocess_data(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Identical logic to original, using Pandas"""
        preprocess_logger = logging.getLogger('BronzeLevelLoader.preprocess')
        preprocess_logger.handlers = self.logger.handlers
        
        try:
            preprocess_logger.info(f"🔧 Starting preprocessing for {data_type}")
            df = df.copy()
            df['annee'] = pd.to_datetime(df['seance']).dt.year
            
            numeric_cols = {
                'cotations': ['ouverture', 'cloture', 'plus_bas', 'plus_haut', 
                            'quantite_negociee', 'nb_transaction', 'capitaux'],
                'indices': ['indice_jour', 'indice_veille', 'variation_veille',
                          'indice_plus_haut', 'indice_plus_bas', 'indice_ouv']
            }.get(data_type)
            
            conversion_stats = {}
            for col_name in numeric_cols:
                if col_name in df.columns:
                    non_null_before = df[col_name].notna().sum()
                    if df[col_name].dtype == object:
                        df[col_name] = df[col_name].astype(str).str.replace(',', '.')
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    non_null_after = df[col_name].notna().sum()
                    conversion_stats[col_name] = {
                        'total': len(df[col_name]),
                        'null_before': len(df[col_name]) - non_null_before,
                        'null_after': len(df[col_name]) - non_null_after,
                        'failed_conversion': non_null_before - non_null_after
                    }
            
            # Convert non-numeric columns to string to prevent Parquet errors
            if data_type == 'cotations':
                for col in ['groupe', 'code', 'valeur']:
                    if col in df.columns:
                        df[col] = df[col].astype(str)
            
            preprocess_logger.debug(f"📊 Conversion stats:\n{json.dumps(conversion_stats, indent=2, cls=NumpyEncoder)}")
            preprocess_logger.info(f"✅ Preprocessed {len(df):,} rows")
            return df
            
        except Exception as e:
            preprocess_logger.error("❌ Preprocessing failed", exc_info=True)
            raise

    @timed_operation("CSV File Reading")
    def _read_csv_with_fallback(self, file_path: str, data_type: str) -> pd.DataFrame:
        """Identical CSV reading logic with Pandas"""
        file_logger = logging.getLogger('BronzeLevelLoader.file_io')
        file_logger.handlers = self.logger.handlers
        
        try:
            file_logger.info(f"📂 Reading {data_type} from: {file_path}")
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Attempt standard read
            try:
                df_preview = pd.read_csv(file_path, nrows=0)
                date_col = next((col for col in ['SEANCE', 'seance', 'Seance', 'DATE', 'date', 'Date'] 
                              if col in df_preview.columns), None)
                
                if date_col:
                    df = pd.read_csv(
                        file_path,
                        parse_dates=[date_col],
                        thousands=',',
                        decimal='.',
                        usecols=lambda c: c.strip() and not c.startswith('Unnamed')
                    )
                else:
                    df = pd.read_csv(
                        file_path,
                        thousands=',',
                        decimal='.',
                        usecols=lambda c: c.strip() and not c.startswith('Unnamed')
                    )
                
                # Case analysis and standardization
                self._analyze_column_case(df, data_type)
                df.columns = df.columns.str.lower()
                self._log_dataframe_stats(df, f"{data_type} (raw)")
                return df
                
            except Exception as e:
                file_logger.warning(f"⚠️ Standard read failed, trying fallback: {str(e)}")
                df_preview = pd.read_csv(file_path, nrows=0)
                date_col = next((col for col in ['SEANCE', 'seance', 'Seance', 'DATE', 'date', 'Date'] 
                              if col in df_preview.columns), None)
                
                if date_col:
                    df = pd.read_csv(
                        file_path,
                        parse_dates=[date_col],
                        low_memory=False,
                        thousands=',',
                        decimal='.',
                        usecols=lambda c: c.strip() and not c.startswith('Unnamed')
                    )
                else:
                    df = pd.read_csv(
                        file_path,
                        low_memory=False,
                        thousands=',',
                        decimal='.',
                        usecols=lambda c: c.strip() and not c.startswith('Unnamed')
                    )
                
                self._analyze_column_case(df, data_type)
                df.columns = df.columns.str.lower()
                file_logger.info("✅ Successfully read with fallback")
                self._log_dataframe_stats(df, f"{data_type} (raw)")
                return df
                
        except Exception as e:
            file_logger.error("❌ Failed to read CSV", exc_info=True)
            raise

    @timed_operation("Get Case Analysis")
    def get_case_analysis(self, data_type: str = None) -> dict:
        """Unchanged from original"""
        if not hasattr(self, '_case_analyses'):
            self.logger.warning("⚠️ No case analyses available")
            return {}
        return self._case_analyses.get(data_type, {}) if data_type else self._case_analyses

    def _safe_write_parquet(self, df: pd.DataFrame, path: str, max_retries: int = 3):
        """Robust atomic file writing with temp files"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        for attempt in range(1, max_retries + 1):
            try:
                # Write to temp directory first
                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
                    temp_path = tmp.name
                    
                df.to_parquet(temp_path)
                
                # Atomic move (works across filesystems)
                shutil.move(temp_path, path)
                
                self.logger.info(f"💾 Successfully saved via atomic write: {path}")
                return
            except PermissionError as e:
                # Clean up temp file on failure
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
                        
                if attempt < max_retries:
                    sleep_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(
                        f"🔒 Permission denied (attempt {attempt}/{max_retries}). "
                        f"Retrying in {sleep_time}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    self.logger.error("❌ Atomic write failed after retries")
                    raise
            except Exception as e:
                self.logger.error(f"❌ Unexpected write error: {str(e)}")
                raise

    @timed_operation("Bronze Data Loading")
    def load_bronze_data(self):
        """Main workflow with robust file handling for Windows"""
        start_time = datetime.now()
        self.logger.info("🏁 Starting Bronze Layer Load (Pandas)")
        
        loading_stats = {
            "cotations": {"input_rows": 0, "output_rows": 0, "duration": 0},
            "indices": {"input_rows": 0, "output_rows": 0, "duration": 0},
            "total_duration": 0
        }

        try:
            #========== COTATIONS ==========
            cotations_start = datetime.now()
            self.logger.info("📊 Processing COTATIONS data")
            
            # Read cotations CSV - use the correct data directory (backend/data/input/)
            current_file = os.path.abspath(__file__)
            bronze_layer_dir = os.path.dirname(current_file)
            etl_backend_dir = os.path.dirname(os.path.dirname(bronze_layer_dir))
            cotations_path = os.path.join(etl_backend_dir, 'data', 'input', 'cotations', 'cotations.csv')
            
            # Debug path resolution
            self.logger.info(f"🔍 Path Debug:")
            self.logger.info(f"  • Current file: {current_file}")
            self.logger.info(f"  • Bronze layer dir: {bronze_layer_dir}")
            self.logger.info(f"  • ETL backend dir: {etl_backend_dir}")
            self.logger.info(f"  • Final cotations path: {cotations_path}")
            self.logger.info(f"  • Path exists: {os.path.exists(cotations_path)}")
            
            self.logger.info(f"📂 Loading from: {cotations_path}")
            
            cotations_pdf = self._read_csv_with_fallback(cotations_path, 'cotations')
            loading_stats["cotations"]["input_rows"] = len(cotations_pdf)
            
            # Preprocess
            self.logger.info(f"🔧 Preprocessing {len(cotations_pdf):,} rows")
            cotations_pdf = self._preprocess_data(cotations_pdf, 'cotations')
            
            cotations_count = len(cotations_pdf)
            loading_stats["cotations"]["output_rows"] = cotations_count
            cotations_duration = (datetime.now() - cotations_start).total_seconds()
            loading_stats["cotations"]["duration"] = cotations_duration
            
            self.logger.info(
                f"✅ Cotations processing complete:\n"
                f"  • Rows processed: {cotations_count:,}\n"
                f"  • Duration: {cotations_duration:.2f}s"
            )

            #========== INDICES ==========
            indices_start = datetime.now()
            self.logger.info("📈 Processing INDICES data")
            
            # Read indices CSV - use the correct data directory (backend/data/input/)
            indices_path = os.path.join(etl_backend_dir, 'data', 'input', 'indices', 'indices.csv')
            self.logger.info(f"📂 Loading from: {indices_path}")
            
            indices_pdf = self._read_csv_with_fallback(indices_path, 'indices')
            loading_stats["indices"]["input_rows"] = len(indices_pdf)
            
            # Preprocess
            self.logger.info(f"🔧 Preprocessing {len(indices_pdf):,} rows")
            indices_pdf = self._preprocess_data(indices_pdf, 'indices')
            
            indices_count = len(indices_pdf)
            loading_stats["indices"]["output_rows"] = indices_count
            indices_duration = (datetime.now() - indices_start).total_seconds()
            loading_stats["indices"]["duration"] = indices_duration
            
            self.logger.info(
                f"✅ Indices processing complete:\n"
                f"  • Rows processed: {indices_count:,}\n"
                f"  • Duration: {indices_duration:.2f}s"
            )

            #========== SAVING TO BRONZE LAYER ==========
            self.logger.info("💾 Saving data to Bronze Layer")
            
            # Define paths
            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')

            # Save to Parquet with robust error handling
            saving_start = datetime.now()
            self.logger.info(f"💿 Saving cotations to Parquet: {bronze_cotations_path}")
            self._safe_write_parquet(cotations_pdf, bronze_cotations_path)
            
            self.logger.info(f"💿 Saving indices to Parquet: {bronze_indices_path}")
            self._safe_write_parquet(indices_pdf, bronze_indices_path)
            
            # Save to PostgreSQL
            self.logger.info("🐘 Saving cotations to PostgreSQL")
            self._save_to_postgres(cotations_pdf, 'bronze_cotations')
            
            self.logger.info("🐘 Saving indices to PostgreSQL")
            self._save_to_postgres(indices_pdf, 'bronze_indices')
            
            saving_duration = (datetime.now() - saving_start).total_seconds()
            
            # Finalize statistics
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            loading_stats["total_duration"] = total_duration
            
            # Log completion with detailed statistics
            self.logger.info(
                f"🏁 Bronze Layer Load completed in {total_duration:.2f} seconds\n"
                f"📊 Performance Summary:\n"
                f"  • Cotations: {cotations_count:,} rows in {cotations_duration:.2f}s\n"
                f"  • Indices: {indices_count:,} rows in {indices_duration:.2f}s\n"
                f"  • Saving: {saving_duration:.2f}s"
            )
            
            # Log case analysis summary
            if hasattr(self, '_case_analyses'):
                self.logger.info("📝 Column Case Analysis Summary:")
                for data_type, analysis in self._case_analyses.items():
                    self.logger.info(
                        f"  • {data_type}: {analysis.get('dominant_pattern', 'unknown')} pattern, "
                        f"standardization {'needed' if analysis.get('needs_standardization', False) else 'not needed'}"
                    )
            
            # Detailed stats as JSON
            self.logger.debug(f"📈 Detailed loading statistics:\n{json.dumps(loading_stats, indent=2)}")

            return cotations_pdf, indices_pdf

        except Exception as e:
            self.logger.error("❌ Bronze Layer Data Load failed", exc_info=True)
            self.logger.error(f"📉 Partial statistics:\n{json.dumps(loading_stats, indent=2)}")
            raise

    @timed_operation("PostgreSQL Save")
    def _save_to_postgres(self, df: pd.DataFrame, table_name: str):
        """Pandas-to-SQL with chunking"""
        db_logger = logging.getLogger('BronzeLevelLoader.postgres')
        db_logger.handlers = self.logger.handlers
        
        try:
            start_time = datetime.now()
            row_count = len(df)
            db_logger.info(f"💾 Saving {row_count:,} rows to {table_name}")
            
            # Use chunksize for better performance
            chunksize = 10000
            total_rows = 0
            for i in range(0, len(df), chunksize):
                chunk = df[i:i+chunksize]
                if_exists = 'replace' if i == 0 else 'append'
                chunk.to_sql(
                    table_name,
                    self.db_engine,
                    if_exists=if_exists,
                    index=False,
                    method='multi'
                )
                total_rows += len(chunk)
                db_logger.debug(f"  • Saved chunk: {i+1}-{i+len(chunk)}")
            
            duration = (datetime.now() - start_time).total_seconds()
            rows_per_second = total_rows / max(duration, 0.001)  # Avoid division by zero
            db_logger.info(
                f"✅ Saved to PostgreSQL:\n"
                f"  • Rows: {total_rows:,}\n"
                f"  • Duration: {duration:.2f}s\n"
                f"  • Throughput: {int(rows_per_second):,} rows/s"
            )
        except Exception as e:
            db_logger.error(f"❌ Failed to save to {table_name}", exc_info=True)
            raise

    @timed_operation("Year Data Extraction")
    def extract_year_data(self, year: int):
        """Read from Parquet and filter by year"""
        extract_logger = logging.getLogger('BronzeLevelLoader.extract')
        extract_logger.handlers = self.logger.handlers
        
        try:
            # Read Parquet files
            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')
            
            cotations = pd.read_parquet(bronze_cotations_path)
            indices = pd.read_parquet(bronze_indices_path)
            
            # Filter by year
            cotations_year = cotations[cotations['annee'] == year]
            indices_year = indices[indices['annee'] == year]
            
            extract_logger.info(
                f"✅ Extracted {len(cotations_year):,} cotations and {len(indices_year):,} indices for {year}"
            )
            return cotations_year, indices_year
            
        except Exception as e:
            extract_logger.error(f"❌ Extraction failed for {year}", exc_info=True)
            raise

def main():
    """Main execution function with comprehensive error handling"""
    main_logger = logging.getLogger('BronzeLevelLoader.main')
    
    # Configure basic logging for main function
    if not main_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s')
        handler.setFormatter(formatter)
        main_logger.addHandler(handler)
        main_logger.setLevel(logging.INFO)
    
    try:
        main_logger.info("🚀 Starting Bronze Level data processing")
        
        # Create loader instance
        loader = BronzeLevelLoader()
        
        # Load full dataset to Bronze layer
        main_logger.info("💽 Loading data to Bronze layer")
        df_cotations, df_indices = loader.load_bronze_data()
        
        # Display case analysis results
        main_logger.info("🔍 Column Case Analysis Results:")
        case_analyses = loader.get_case_analysis()
        
        for data_type, analysis in case_analyses.items():
            main_logger.info(
                f"\n=== {data_type.upper()} CASE ANALYSIS ===\n"
                f"  • Total columns: {analysis.get('total_columns', 0)}\n"
                f"  • Dominant pattern: {analysis.get('dominant_pattern', 'unknown')}\n"
                f"  • Needs standardization: {'Yes' if analysis.get('needs_standardization', False) else 'No'}"
            )
            
            # Show pattern breakdown
            patterns = analysis.get('case_patterns', {})
            for pattern_name, columns in patterns.items():
                if columns:
                    main_logger.info(f"  • {pattern_name}: {len(columns)} columns")
                    main_logger.debug(f"    {columns}")
        
        # Example: Extract data for specific year
        year_to_extract = 2020
        main_logger.info(f"📅 Extracting data for year {year_to_extract}")
        cotations_year, indices_year = loader.extract_year_data(year_to_extract)
        
        main_logger.info("✅ Process completed successfully")
        
        return 0  # Success exit code
        
    except FileNotFoundError as e:
        main_logger.error(f"❌ File not found: {str(e)}")
        return 1  # Error exit code
        
    except Exception as e:
        main_logger.critical(f"💥 Fatal error in main execution: {str(e)}", exc_info=True)
        return 2  # Critical error exit code

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)



    @timed_operation("Get Case Analysis")

    def get_case_analysis(self, data_type: str = None) -> dict:

        """Unchanged from original"""

        if not hasattr(self, '_case_analyses'):

            self.logger.warning("⚠️ No case analyses available")

            return {}

        return self._case_analyses.get(data_type, {}) if data_type else self._case_analyses



    def _safe_write_parquet(self, df: pd.DataFrame, path: str, max_retries: int = 3):

        """Robust atomic file writing with temp files"""

        os.makedirs(os.path.dirname(path), exist_ok=True)

        

        for attempt in range(1, max_retries + 1):

            try:

                # Write to temp directory first

                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:

                    temp_path = tmp.name

                    

                df.to_parquet(temp_path)

                

                # Atomic move (works across filesystems)

                shutil.move(temp_path, path)

                

                self.logger.info(f"💾 Successfully saved via atomic write: {path}")

                return

            except PermissionError as e:

                # Clean up temp file on failure

                if os.path.exists(temp_path):

                    try:

                        os.remove(temp_path)

                    except:

                        pass

                        

                if attempt < max_retries:

                    sleep_time = 2 ** attempt  # Exponential backoff

                    self.logger.warning(

                        f"🔒 Permission denied (attempt {attempt}/{max_retries}). "

                        f"Retrying in {sleep_time}s..."

                    )

                    time.sleep(sleep_time)

                else:

                    self.logger.error("❌ Atomic write failed after retries")

                    raise

            except Exception as e:

                self.logger.error(f"❌ Unexpected write error: {str(e)}")

                raise



    @timed_operation("Bronze Data Loading")

    def load_bronze_data(self):

        """Main workflow with robust file handling for Windows"""

        start_time = datetime.now()

        self.logger.info("🏁 Starting Bronze Layer Load (Pandas)")

        

        loading_stats = {

            "cotations": {"input_rows": 0, "output_rows": 0, "duration": 0},

            "indices": {"input_rows": 0, "output_rows": 0, "duration": 0},

            "total_duration": 0

        }



        try:

            #========== COTATIONS ==========

            cotations_start = datetime.now()

            self.logger.info("📊 Processing COTATIONS data")

            

            # Read cotations CSV

            cotations_path = os.path.join(PATHS['input_data'], 'cotations', 'cotations.csv')

            self.logger.info(f"📂 Loading from: {cotations_path}")

            

            cotations_pdf = self._read_csv_with_fallback(cotations_path, 'cotations')

            loading_stats["cotations"]["input_rows"] = len(cotations_pdf)

            

            # Preprocess

            self.logger.info(f"🔧 Preprocessing {len(cotations_pdf):,} rows")

            cotations_pdf = self._preprocess_data(cotations_pdf, 'cotations')

            

            cotations_count = len(cotations_pdf)

            loading_stats["cotations"]["output_rows"] = cotations_count

            cotations_duration = (datetime.now() - cotations_start).total_seconds()

            loading_stats["cotations"]["duration"] = cotations_duration

            

            self.logger.info(

                f"✅ Cotations processing complete:\n"

                f"  • Rows processed: {cotations_count:,}\n"

                f"  • Duration: {cotations_duration:.2f}s"

            )



            #========== INDICES ==========

            indices_start = datetime.now()

            self.logger.info("📈 Processing INDICES data")

            

            # Read indices CSV

            indices_path = os.path.join(PATHS['input_data'], 'indices', 'indices.csv')

            self.logger.info(f"📂 Loading from: {indices_path}")

            

            indices_pdf = self._read_csv_with_fallback(indices_path, 'indices')

            loading_stats["indices"]["input_rows"] = len(indices_pdf)

            

            # Preprocess

            self.logger.info(f"🔧 Preprocessing {len(indices_pdf):,} rows")

            indices_pdf = self._preprocess_data(indices_pdf, 'indices')

            

            indices_count = len(indices_pdf)

            loading_stats["indices"]["output_rows"] = indices_count

            indices_duration = (datetime.now() - indices_start).total_seconds()

            loading_stats["indices"]["duration"] = indices_duration

            

            self.logger.info(

                f"✅ Indices processing complete:\n"

                f"  • Rows processed: {indices_count:,}\n"

                f"  • Duration: {indices_duration:.2f}s"

            )



            #========== SAVING TO BRONZE LAYER ==========

            self.logger.info("💾 Saving data to Bronze Layer")

            

            # Define paths

            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')

            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')



            # Save to Parquet with robust error handling

            saving_start = datetime.now()

            self.logger.info(f"💿 Saving cotations to Parquet: {bronze_cotations_path}")

            self._safe_write_parquet(cotations_pdf, bronze_cotations_path)

            

            self.logger.info(f"💿 Saving indices to Parquet: {bronze_indices_path}")

            self._safe_write_parquet(indices_pdf, bronze_indices_path)

            

            # Save to PostgreSQL

            self.logger.info("🐘 Saving cotations to PostgreSQL")

            self._save_to_postgres(cotations_pdf, 'bronze_cotations')

            

            self.logger.info("🐘 Saving indices to PostgreSQL")

            self._save_to_postgres(indices_pdf, 'bronze_indices')

            

            saving_duration = (datetime.now() - saving_start).total_seconds()

            

            # Finalize statistics

            end_time = datetime.now()

            total_duration = (end_time - start_time).total_seconds()

            loading_stats["total_duration"] = total_duration

            

            # Log completion with detailed statistics

            self.logger.info(

                f"🏁 Bronze Layer Load completed in {total_duration:.2f} seconds\n"

                f"📊 Performance Summary:\n"

                f"  • Cotations: {cotations_count:,} rows in {cotations_duration:.2f}s\n"

                f"  • Indices: {indices_count:,} rows in {indices_duration:.2f}s\n"

                f"  • Saving: {saving_duration:.2f}s"

            )

            

            # Log case analysis summary

            if hasattr(self, '_case_analyses'):

                self.logger.info("📝 Column Case Analysis Summary:")

                for data_type, analysis in self._case_analyses.items():

                    self.logger.info(

                        f"  • {data_type}: {analysis.get('dominant_pattern', 'unknown')} pattern, "

                        f"standardization {'needed' if analysis.get('needs_standardization', False) else 'not needed'}"

                    )

            

            # Detailed stats as JSON

            self.logger.debug(f"📈 Detailed loading statistics:\n{json.dumps(loading_stats, indent=2)}")



            return cotations_pdf, indices_pdf



        except Exception as e:

            self.logger.error("❌ Bronze Layer Data Load failed", exc_info=True)

            self.logger.error(f"📉 Partial statistics:\n{json.dumps(loading_stats, indent=2)}")

            raise



    @timed_operation("PostgreSQL Save")

    def _save_to_postgres(self, df: pd.DataFrame, table_name: str):

        """Pandas-to-SQL with chunking"""

        db_logger = logging.getLogger('BronzeLevelLoader.postgres')

        db_logger.handlers = self.logger.handlers

        

        try:

            start_time = datetime.now()

            row_count = len(df)

            db_logger.info(f"💾 Saving {row_count:,} rows to {table_name}")

            

            # Use chunksize for better performance

            chunksize = 10000

            total_rows = 0

            for i in range(0, len(df), chunksize):

                chunk = df[i:i+chunksize]

                if_exists = 'replace' if i == 0 else 'append'

                chunk.to_sql(

                    table_name,

                    self.db_engine,

                    if_exists=if_exists,

                    index=False,

                    method='multi'

                )

                total_rows += len(chunk)

                db_logger.debug(f"  • Saved chunk: {i+1}-{i+len(chunk)}")

            

            duration = (datetime.now() - start_time).total_seconds()

            rows_per_second = total_rows / max(duration, 0.001)  # Avoid division by zero

            db_logger.info(

                f"✅ Saved to PostgreSQL:\n"

                f"  • Rows: {total_rows:,}\n"

                f"  • Duration: {duration:.2f}s\n"

                f"  • Throughput: {int(rows_per_second):,} rows/s"

            )

        except Exception as e:

            db_logger.error(f"❌ Failed to save to {table_name}", exc_info=True)

            raise



    @timed_operation("Year Data Extraction")

    def extract_year_data(self, year: int):

        """Read from Parquet and filter by year"""

        extract_logger = logging.getLogger('BronzeLevelLoader.extract')

        extract_logger.handlers = self.logger.handlers

        

        try:

            # Read Parquet files

            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')

            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')

            

            cotations = pd.read_parquet(bronze_cotations_path)

            indices = pd.read_parquet(bronze_indices_path)

            

            # Filter by year

            cotations_year = cotations[cotations['annee'] == year]

            indices_year = indices[indices['annee'] == year]

            

            extract_logger.info(

                f"✅ Extracted {len(cotations_year):,} cotations and {len(indices_year):,} indices for {year}"

            )

            return cotations_year, indices_year

            

        except Exception as e:

            extract_logger.error(f"❌ Extraction failed for {year}", exc_info=True)

            raise



def main():

    """Main execution function with comprehensive error handling"""

    main_logger = logging.getLogger('BronzeLevelLoader.main')

    

    # Configure basic logging for main function

    if not main_logger.handlers:

        handler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s')

        handler.setFormatter(formatter)

        main_logger.addHandler(handler)

        main_logger.setLevel(logging.INFO)

    

    try:

        main_logger.info("🚀 Starting Bronze Level data processing")

        

        # Create loader instance

        loader = BronzeLevelLoader()

        

        # Load full dataset to Bronze layer

        main_logger.info("💽 Loading data to Bronze layer")

        df_cotations, df_indices = loader.load_bronze_data()

        

        # Display case analysis results

        main_logger.info("🔍 Column Case Analysis Results:")

        case_analyses = loader.get_case_analysis()

        

        for data_type, analysis in case_analyses.items():

            main_logger.info(

                f"\n=== {data_type.upper()} CASE ANALYSIS ===\n"

                f"  • Total columns: {analysis.get('total_columns', 0)}\n"

                f"  • Dominant pattern: {analysis.get('dominant_pattern', 'unknown')}\n"

                f"  • Needs standardization: {'Yes' if analysis.get('needs_standardization', False) else 'No'}"

            )

            

            # Show pattern breakdown

            patterns = analysis.get('case_patterns', {})

            for pattern_name, columns in patterns.items():

                if columns:

                    main_logger.info(f"  • {pattern_name}: {len(columns)} columns")

                    main_logger.debug(f"    {columns}")

        

        # Example: Extract data for specific year

        year_to_extract = 2020

        main_logger.info(f"📅 Extracting data for year {year_to_extract}")

        cotations_year, indices_year = loader.extract_year_data(year_to_extract)

        

        main_logger.info("✅ Process completed successfully")

        

        return 0  # Success exit code

        

    except FileNotFoundError as e:

        main_logger.error(f"❌ File not found: {str(e)}")

        return 1  # Error exit code

        

    except Exception as e:

        main_logger.critical(f"💥 Fatal error in main execution: {str(e)}", exc_info=True)

        return 2  # Critical error exit code



if __name__ == "__main__":

    exit_code = main()

    exit(exit_code)





    @timed_operation("Get Case Analysis")

    def get_case_analysis(self, data_type: str = None) -> dict:

        """Unchanged from original"""

        if not hasattr(self, '_case_analyses'):

            self.logger.warning("⚠️ No case analyses available")

            return {}

        return self._case_analyses.get(data_type, {}) if data_type else self._case_analyses



    def _safe_write_parquet(self, df: pd.DataFrame, path: str, max_retries: int = 3):

        """Robust atomic file writing with temp files"""

        os.makedirs(os.path.dirname(path), exist_ok=True)

        

        for attempt in range(1, max_retries + 1):

            try:

                # Write to temp directory first

                with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:

                    temp_path = tmp.name

                    

                df.to_parquet(temp_path)

                

                # Atomic move (works across filesystems)

                shutil.move(temp_path, path)

                

                self.logger.info(f"💾 Successfully saved via atomic write: {path}")

                return

            except PermissionError as e:

                # Clean up temp file on failure

                if os.path.exists(temp_path):

                    try:

                        os.remove(temp_path)

                    except:

                        pass

                        

                if attempt < max_retries:

                    sleep_time = 2 ** attempt  # Exponential backoff

                    self.logger.warning(

                        f"🔒 Permission denied (attempt {attempt}/{max_retries}). "

                        f"Retrying in {sleep_time}s..."

                    )

                    time.sleep(sleep_time)

                else:

                    self.logger.error("❌ Atomic write failed after retries")

                    raise

            except Exception as e:

                self.logger.error(f"❌ Unexpected write error: {str(e)}")

                raise



    @timed_operation("Bronze Data Loading")

    def load_bronze_data(self):

        """Main workflow with robust file handling for Windows"""

        start_time = datetime.now()

        self.logger.info("🏁 Starting Bronze Layer Load (Pandas)")

        

        loading_stats = {

            "cotations": {"input_rows": 0, "output_rows": 0, "duration": 0},

            "indices": {"input_rows": 0, "output_rows": 0, "duration": 0},

            "total_duration": 0

        }



        try:

            #========== COTATIONS ==========

            cotations_start = datetime.now()

            self.logger.info("📊 Processing COTATIONS data")

            

            # Read cotations CSV

            cotations_path = os.path.join(PATHS['input_data'], 'cotations', 'cotations.csv')

            self.logger.info(f"📂 Loading from: {cotations_path}")

            

            cotations_pdf = self._read_csv_with_fallback(cotations_path, 'cotations')

            loading_stats["cotations"]["input_rows"] = len(cotations_pdf)

            

            # Preprocess

            self.logger.info(f"🔧 Preprocessing {len(cotations_pdf):,} rows")

            cotations_pdf = self._preprocess_data(cotations_pdf, 'cotations')

            

            cotations_count = len(cotations_pdf)

            loading_stats["cotations"]["output_rows"] = cotations_count

            cotations_duration = (datetime.now() - cotations_start).total_seconds()

            loading_stats["cotations"]["duration"] = cotations_duration

            

            self.logger.info(

                f"✅ Cotations processing complete:\n"

                f"  • Rows processed: {cotations_count:,}\n"

                f"  • Duration: {cotations_duration:.2f}s"

            )



            #========== INDICES ==========

            indices_start = datetime.now()

            self.logger.info("📈 Processing INDICES data")

            

            # Read indices CSV

            indices_path = os.path.join(PATHS['input_data'], 'indices', 'indices.csv')

            self.logger.info(f"📂 Loading from: {indices_path}")

            

            indices_pdf = self._read_csv_with_fallback(indices_path, 'indices')

            loading_stats["indices"]["input_rows"] = len(indices_pdf)

            

            # Preprocess

            self.logger.info(f"🔧 Preprocessing {len(indices_pdf):,} rows")

            indices_pdf = self._preprocess_data(indices_pdf, 'indices')

            

            indices_count = len(indices_pdf)

            loading_stats["indices"]["output_rows"] = indices_count

            indices_duration = (datetime.now() - indices_start).total_seconds()

            loading_stats["indices"]["duration"] = indices_duration

            

            self.logger.info(

                f"✅ Indices processing complete:\n"

                f"  • Rows processed: {indices_count:,}\n"

                f"  • Duration: {indices_duration:.2f}s"

            )



            #========== SAVING TO BRONZE LAYER ==========

            self.logger.info("💾 Saving data to Bronze Layer")

            

            # Define paths

            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')

            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')



            # Save to Parquet with robust error handling

            saving_start = datetime.now()

            self.logger.info(f"💿 Saving cotations to Parquet: {bronze_cotations_path}")

            self._safe_write_parquet(cotations_pdf, bronze_cotations_path)

            

            self.logger.info(f"💿 Saving indices to Parquet: {bronze_indices_path}")

            self._safe_write_parquet(indices_pdf, bronze_indices_path)

            

            # Save to PostgreSQL

            self.logger.info("🐘 Saving cotations to PostgreSQL")

            self._save_to_postgres(cotations_pdf, 'bronze_cotations')

            

            self.logger.info("🐘 Saving indices to PostgreSQL")

            self._save_to_postgres(indices_pdf, 'bronze_indices')

            

            saving_duration = (datetime.now() - saving_start).total_seconds()

            

            # Finalize statistics

            end_time = datetime.now()

            total_duration = (end_time - start_time).total_seconds()

            loading_stats["total_duration"] = total_duration

            

            # Log completion with detailed statistics

            self.logger.info(

                f"🏁 Bronze Layer Load completed in {total_duration:.2f} seconds\n"

                f"📊 Performance Summary:\n"

                f"  • Cotations: {cotations_count:,} rows in {cotations_duration:.2f}s\n"

                f"  • Indices: {indices_count:,} rows in {indices_duration:.2f}s\n"

                f"  • Saving: {saving_duration:.2f}s"

            )

            

            # Log case analysis summary

            if hasattr(self, '_case_analyses'):

                self.logger.info("📝 Column Case Analysis Summary:")

                for data_type, analysis in self._case_analyses.items():

                    self.logger.info(

                        f"  • {data_type}: {analysis.get('dominant_pattern', 'unknown')} pattern, "

                        f"standardization {'needed' if analysis.get('needs_standardization', False) else 'not needed'}"

                    )

            

            # Detailed stats as JSON

            self.logger.debug(f"📈 Detailed loading statistics:\n{json.dumps(loading_stats, indent=2)}")



            return cotations_pdf, indices_pdf



        except Exception as e:

            self.logger.error("❌ Bronze Layer Data Load failed", exc_info=True)

            self.logger.error(f"📉 Partial statistics:\n{json.dumps(loading_stats, indent=2)}")

            raise



    @timed_operation("PostgreSQL Save")

    def _save_to_postgres(self, df: pd.DataFrame, table_name: str):

        """Pandas-to-SQL with chunking"""

        db_logger = logging.getLogger('BronzeLevelLoader.postgres')

        db_logger.handlers = self.logger.handlers

        

        try:

            start_time = datetime.now()

            row_count = len(df)

            db_logger.info(f"💾 Saving {row_count:,} rows to {table_name}")

            

            # Use chunksize for better performance

            chunksize = 10000

            total_rows = 0

            for i in range(0, len(df), chunksize):

                chunk = df[i:i+chunksize]

                if_exists = 'replace' if i == 0 else 'append'

                chunk.to_sql(

                    table_name,

                    self.db_engine,

                    if_exists=if_exists,

                    index=False,

                    method='multi'

                )

                total_rows += len(chunk)

                db_logger.debug(f"  • Saved chunk: {i+1}-{i+len(chunk)}")

            

            duration = (datetime.now() - start_time).total_seconds()

            rows_per_second = total_rows / max(duration, 0.001)  # Avoid division by zero

            db_logger.info(

                f"✅ Saved to PostgreSQL:\n"

                f"  • Rows: {total_rows:,}\n"

                f"  • Duration: {duration:.2f}s\n"

                f"  • Throughput: {int(rows_per_second):,} rows/s"

            )

        except Exception as e:

            db_logger.error(f"❌ Failed to save to {table_name}", exc_info=True)

            raise



    @timed_operation("Year Data Extraction")

    def extract_year_data(self, year: int):

        """Read from Parquet and filter by year"""

        extract_logger = logging.getLogger('BronzeLevelLoader.extract')

        extract_logger.handlers = self.logger.handlers

        

        try:

            # Read Parquet files

            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')

            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')

            

            cotations = pd.read_parquet(bronze_cotations_path)

            indices = pd.read_parquet(bronze_indices_path)

            

            # Filter by year

            cotations_year = cotations[cotations['annee'] == year]

            indices_year = indices[indices['annee'] == year]

            

            extract_logger.info(

                f"✅ Extracted {len(cotations_year):,} cotations and {len(indices_year):,} indices for {year}"

            )

            return cotations_year, indices_year

            

        except Exception as e:

            extract_logger.error(f"❌ Extraction failed for {year}", exc_info=True)

            raise



def main():

    """Main execution function with comprehensive error handling"""

    main_logger = logging.getLogger('BronzeLevelLoader.main')

    

    # Configure basic logging for main function

    if not main_logger.handlers:

        handler = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s')

        handler.setFormatter(formatter)

        main_logger.addHandler(handler)

        main_logger.setLevel(logging.INFO)

    

    try:

        main_logger.info("🚀 Starting Bronze Level data processing")

        

        # Create loader instance

        loader = BronzeLevelLoader()

        

        # Load full dataset to Bronze layer

        main_logger.info("💽 Loading data to Bronze layer")

        df_cotations, df_indices = loader.load_bronze_data()

        

        # Display case analysis results

        main_logger.info("🔍 Column Case Analysis Results:")

        case_analyses = loader.get_case_analysis()

        

        for data_type, analysis in case_analyses.items():

            main_logger.info(

                f"\n=== {data_type.upper()} CASE ANALYSIS ===\n"

                f"  • Total columns: {analysis.get('total_columns', 0)}\n"

                f"  • Dominant pattern: {analysis.get('dominant_pattern', 'unknown')}\n"

                f"  • Needs standardization: {'Yes' if analysis.get('needs_standardization', False) else 'No'}"

            )

            

            # Show pattern breakdown

            patterns = analysis.get('case_patterns', {})

            for pattern_name, columns in patterns.items():

                if columns:

                    main_logger.info(f"  • {pattern_name}: {len(columns)} columns")

                    main_logger.debug(f"    {columns}")

        

        # Example: Extract data for specific year

        year_to_extract = 2020

        main_logger.info(f"📅 Extracting data for year {year_to_extract}")

        cotations_year, indices_year = loader.extract_year_data(year_to_extract)

        

        main_logger.info("✅ Process completed successfully")

        

        return 0  # Success exit code

        

    except FileNotFoundError as e:

        main_logger.error(f"❌ File not found: {str(e)}")

        return 1  # Error exit code

        

    except Exception as e:

        main_logger.critical(f"💥 Fatal error in main execution: {str(e)}", exc_info=True)

        return 2  # Critical error exit code



if __name__ == "__main__":

    exit_code = main()

    exit(exit_code)