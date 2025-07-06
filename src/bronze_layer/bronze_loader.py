import os
import logging
from datetime import datetime
import pandas as pd
import json
from pathlib import Path
import numpy as np
from timeit import default_timer as timer
from functools import wraps

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, col

from src.utils.db_connection import DatabaseConnector
from src.bronze_layer.schemas import create_bronze_schemas
from config.settings import PATHS
from src.utils.spark_session import SparkSessionManager

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
    """
    Decorator to log operation timing and status
    
    Args:
        operation_name (str, optional): Name of the operation. If None, uses function name.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Determine operation name
            op_name = operation_name if operation_name else func.__name__.replace('_', ' ').title()
            
            # Create logger instance
            op_logger = logging.getLogger(f'BronzeLevelLoader.operations.{op_name.replace(" ", "_").lower()}')
            
            # Log operation start
            op_logger.info(f"🚀 Starting operation: {op_name}")
            start_time = timer()
            
            try:
                # Execute the function
                result = func(*args, **kwargs)
                
                # Log operation completion
                duration = timer() - start_time
                op_logger.info(f"✅ Completed {op_name} in {duration:.3f} seconds")
                
                return result
                
            except Exception as e:
                # Log operation failure
                duration = timer() - start_time
                op_logger.error(f"❌ Failed {op_name} after {duration:.3f} seconds", exc_info=True)
                raise
                
        return wrapper
    return decorator

class BronzeLevelLoader:
    def __init__(self):
        # Initialize logging
        self.logger = self._setup_logger()
        
        # Initialize Spark and Database managers
        self.spark_manager = SparkSessionManager()
        self.spark = self.spark_manager.get_spark_session()
        self.db_connector = DatabaseConnector()
        
        # Load predefined schemas
        self.cotations_schema, self.indices_schema = create_bronze_schemas()
        
        # Log initialization complete
        self.logger.info("🏁 BronzeLevelLoader initialized successfully")
        # Convert WindowsPath objects to strings before JSON serialization
        paths_str = {k: str(v) for k, v in PATHS.items()}
        self.logger.debug(f"Configuration paths:\n{json.dumps(paths_str, indent=2)}")

    def _setup_logger(self):
        """
        Set up structured logging configuration
        """
        logger = logging.getLogger('BronzeLevelLoader')
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers to avoid duplicate logs
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # File Handler - detailed logs
        log_file = os.path.join(PATHS['logs'], 'bronze_loader.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
        # Console Handler - important info only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Custom Formatter with visual indicators
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
        
        # Log startup
        startup_logger = logging.getLogger('BronzeLevelLoader.startup')
        startup_logger.handlers = logger.handlers
        startup_logger.info(f"📝 Detailed logs will be written to: {log_file}")
        
        return logger

    @timed_operation("Column Case Analysis")
    def _analyze_column_case(self, df: pd.DataFrame, data_type: str) -> dict:
        """
        Analyze column names to detect case patterns and provide detailed statistics
        
        Args:
            df (pd.DataFrame): Input DataFrame
            data_type (str): 'cotations' or 'indices'
        
        Returns:
            dict: Column case analysis results
        """
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
            
            # Analyze each column
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
            
            # Determine dominant pattern
            stats = case_analysis['statistics']
            max_count = max(stats.values())
            case_analysis['dominant_pattern'] = 'none' if max_count == 0 else \
                next(pattern.replace('_count', '') for pattern, count in stats.items() if count == max_count)
            
            # Check if standardization is needed
            pattern_counts = [count for count in stats.values() if count > 0]
            case_analysis['needs_standardization'] = len(pattern_counts) > 1
            
            # Log analysis summary
            case_logger.info(
                f"📊 Case Analysis Summary - {data_type}:\n"
                f"  • Total columns: {case_analysis['total_columns']}\n"
                f"  • Dominant pattern: {case_analysis['dominant_pattern']}\n"
                f"  • Needs standardization: {'Yes' if case_analysis['needs_standardization'] else 'No'}"
            )
            
            # Log pattern breakdown
            for pattern, columns in case_analysis['case_patterns'].items():
                if columns:
                    case_logger.debug(f"  • {pattern}: {len(columns)} columns - {columns}")
            
            # Log statistics summary
            case_logger.debug(f"📈 Pattern statistics:\n{json.dumps(stats, indent=2)}")
            
            # Warnings for potential issues
            if case_analysis['needs_standardization']:
                case_logger.warning("⚠️ Mixed case patterns detected - consider standardization")
            
            if case_analysis['statistics']['other_count'] > 0:
                case_logger.warning(f"⚠️ Found {case_analysis['statistics']['other_count']} columns with unusual naming patterns")
            
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
        """
        Log comprehensive DataFrame statistics
        """
        stats_logger = logging.getLogger('BronzeLevelLoader.stats')
        stats_logger.handlers = self.logger.handlers
        
        # Basic stats
        row_count = df.count() if isinstance(df, DataFrame) else len(df)
        col_count = len(df.columns)
        
        stats_logger.info(
            f"📋 {name} Statistics:\n"
            f"  • Rows: {row_count:,}\n"
            f"  • Columns: {col_count}"
        )
        
        # Detailed column info and sample data
        if isinstance(df, DataFrame):
            stats_logger.debug(f"📜 Schema:\n{df.schema}")
            
            if row_count > 0:
                sample_df = df.limit(sample_size).toPandas()
                stats_logger.debug(f"🔍 Sample data:\n{sample_df.to_string()}")
        else:
            stats_logger.debug(f"📜 Columns: {df.columns.tolist()}")
            stats_logger.debug(f"📊 Data types:\n{df.dtypes}")
            
            if row_count > 0:
                stats_logger.debug(f"🔍 Sample data:\n{df.head(sample_size).to_string()}")
        
        # Log null counts
        if isinstance(df, pd.DataFrame):
            null_counts = df.isna().sum()
            null_cols = null_counts[null_counts > 0]
            if not null_cols.empty:
                stats_logger.warning(f"⚠️ Null values detected:\n{null_cols}")

    @timed_operation("Data Preprocessing")
    def _preprocess_data(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Preprocess input data with robust type handling and detailed progress logging
        
        Args:
            df (pd.DataFrame): Input DataFrame
            data_type (str): 'cotations' or 'indices'
        
        Returns:
            Preprocessed DataFrame
        """
        preprocess_logger = logging.getLogger('BronzeLevelLoader.preprocess')
        preprocess_logger.handlers = self.logger.handlers
        
        try:
            preprocess_logger.info(f"🔧 Starting preprocessing for {data_type}")
            preprocess_logger.debug(f"Input columns: {df.columns.tolist()}")
            
            # Make a copy to avoid SettingWithCopyWarning
            df = df.copy()
            
            # Add year column
            preprocess_logger.debug("➕ Adding 'annee' column")
            df['annee'] = pd.to_datetime(df['seance']).dt.year
            
            # Define numeric columns based on data type
            if data_type == 'cotations':
                numeric_cols = [
                    'ouverture', 'cloture', 'plus_bas', 'plus_haut', 
                    'quantite_negociee', 'nb_transaction', 'capitaux'
                ]
            elif data_type == 'indices':
                numeric_cols = [
                    'indice_jour', 'indice_veille', 'variation_veille', 
                    'indice_plus_haut', 'indice_plus_bas', 'indice_ouv'
                ]
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
            preprocess_logger.debug(f"🔄 Converting {len(numeric_cols)} numeric columns")
            
            # Process each numeric column
            conversion_stats = {}
            for col_name in numeric_cols:
                if col_name in df.columns:
                    preprocess_logger.debug(f"🔢 Processing column: {col_name}")
                    
                    # Count non-null values before conversion
                    non_null_before = df[col_name].notna().sum()
                    
                    # First ensure it's string type if it contains commas
                    if df[col_name].dtype == object:
                        df[col_name] = df[col_name].astype(str).str.replace(',', '.')
                    
                    # Then convert to numeric
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    
                    # Count non-null values after conversion
                    non_null_after = df[col_name].notna().sum()
                    null_after_conversion = non_null_before - non_null_after
                    
                    # Store conversion stats
                    conversion_stats[col_name] = {
                        'total': int(len(df[col_name])),
                        'null_before': int(len(df[col_name]) - non_null_before),
                        'null_after': int(len(df[col_name]) - non_null_after),
                        'failed_conversion': int(null_after_conversion)
                    }
                    
                    if null_after_conversion > 0:
                        preprocess_logger.warning(
                            f"⚠️ {null_after_conversion} values in '{col_name}' "
                            f"could not be converted to numeric"
                        )
            
            # Log conversion summary
            preprocess_logger.debug(
                f"📊 Numeric conversion summary:\n"
                f"{json.dumps(conversion_stats, indent=2, cls=NumpyEncoder)}"
            )
            
            # Log completion
            preprocess_logger.info(
                f"✅ Successfully preprocessed {len(df):,} {data_type} rows"
            )
            
            return df
        
        except Exception as e:
            preprocess_logger.error("❌ Preprocessing failed", exc_info=True)
            raise

    @timed_operation("CSV File Reading")
    def _read_csv_with_fallback(self, file_path: str, data_type: str) -> pd.DataFrame:
        """
        Read CSV with robust error handling and type inference
        
        Args:
            file_path (str): Path to CSV file
            data_type (str): 'cotations' or 'indices'
            
        Returns:
            pd.DataFrame: Loaded dataframe
        """
        file_logger = logging.getLogger('BronzeLevelLoader.file_io')
        file_logger.handlers = self.logger.handlers
        
        try:
            file_logger.info(f"📂 Reading {data_type} from: {file_path}")
            
            if not os.path.exists(file_path):
                file_logger.error(f"❌ File not found: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")
            
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            file_logger.debug(f"📏 File size: {file_size_mb:.2f} MB")
            
            # Method 1: Standard read with type inference
            try:
                file_logger.debug("🔄 Attempting standard read with type inference")
                
                # First read without date parsing to inspect columns
                df_preview = pd.read_csv(file_path, nrows=0)  # Just headers
                available_cols = df_preview.columns.tolist()
                file_logger.debug(f"📜 Available columns: {available_cols}")
                
                # Determine date column dynamically
                date_col = None
                possible_date_cols = ['SEANCE', 'seance', 'Seance', 'DATE', 'date', 'Date']
                for col in possible_date_cols:
                    if col in available_cols:
                        date_col = col
                        file_logger.debug(f"📅 Found date column: {col}")
                        break
                
                # Read with or without date parsing
                if date_col:
                    df = pd.read_csv(
                        file_path,
                        parse_dates=[date_col],
                        usecols=lambda column: column.strip() != '' and not column.startswith('Unnamed')
                    )
                else:
                    file_logger.warning("⚠️ No date column found, reading without date parsing")
                    df = pd.read_csv(
                        file_path,
                        usecols=lambda column: column.strip() != '' and not column.startswith('Unnamed')
                    )
                
                file_logger.debug("✅ Successfully read with standard approach")
                
                # Analyze column case patterns before standardization
                case_analysis = self._analyze_column_case(df, data_type)
                
                # Store case analysis in instance for potential future use
                if not hasattr(self, '_case_analyses'):
                    self._case_analyses = {}
                self._case_analyses[data_type] = case_analysis
                
                # Convert all column names to lowercase
                df.columns = df.columns.str.lower()
                file_logger.debug("🔄 Columns standardized to lowercase")
                
                # Log DataFrame details
                self._log_dataframe_stats(df, f"{data_type} (raw)")
                
                return df
                
            except Exception as e:
                file_logger.warning(f"⚠️ Standard read failed, trying fallback method: {str(e)}")
                
                # Method 2: Fallback with low_memory=False
                df_preview = pd.read_csv(file_path, nrows=0)
                available_cols = df_preview.columns.tolist()
                file_logger.debug(f"📜 Available columns (fallback): {available_cols}")
                
                # Determine date column dynamically
                date_col = None
                for col in possible_date_cols:
                    if col in available_cols:
                        date_col = col
                        file_logger.debug(f"📅 Found date column (fallback): {col}")
                        break
                
                # Read with or without date parsing
                if date_col:
                    df = pd.read_csv(
                        file_path,
                        parse_dates=[date_col],
                        low_memory=False,
                        usecols=lambda column: column.strip() != '' and not column.startswith('Unnamed')
                    )
                else:
                    file_logger.warning("⚠️ No date column found (fallback), reading without date parsing")
                    df = pd.read_csv(
                        file_path,
                        low_memory=False,
                        usecols=lambda column: column.strip() != '' and not column.startswith('Unnamed')
                    )
                
                # Analyze column case patterns before standardization
                case_analysis = self._analyze_column_case(df, data_type)
                
                # Store case analysis
                if not hasattr(self, '_case_analyses'):
                    self._case_analyses = {}
                self._case_analyses[data_type] = case_analysis
                
                # Convert all column names to lowercase
                df.columns = df.columns.str.lower()
                file_logger.info("✅ Successfully read with fallback method")
                
                # Log DataFrame details
                self._log_dataframe_stats(df, f"{data_type} (raw)")
                
                return df
                
        except Exception as e:
            file_logger.error("❌ Failed to read CSV file", exc_info=True)
            raise

    @timed_operation("Get Case Analysis")
    def get_case_analysis(self, data_type: str = None) -> dict:
        """
        Get column case analysis results
        
        Args:
            data_type (str, optional): Specific data type ('cotations' or 'indices'). 
                                     If None, returns all analyses.
        
        Returns:
            dict: Case analysis results
        """
        if not hasattr(self, '_case_analyses'):
            self.logger.warning("⚠️ No case analyses available. Run load_bronze_data() first.")
            return {}
        
        if data_type:
            return self._case_analyses.get(data_type, {})
        else:
            return self._case_analyses

    @timed_operation("Bronze Data Loading")
    def load_bronze_data(self):
        """
        Load data to Bronze layer with comprehensive error handling and detailed logging
        
        Returns:
            Tuple of DataFrames for cotations and indices
        """
        start_time = datetime.now()
        self.logger.info("🏁 Starting Bronze Layer Data Load")
        self.logger.debug(f"⏱️ Start time: {start_time}")
        
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
            
            # Convert to Spark DataFrame
            self.logger.info("🔄 Converting to Spark DataFrame")
            df_cotations = self.spark.createDataFrame(cotations_pdf, schema=self.cotations_schema)
            
            # Add metadata columns
            self.logger.debug("➕ Adding metadata columns")
            df_cotations = df_cotations.withColumn(
                "ingestion_timestamp", current_timestamp()
            ).withColumn(
                "source_file", lit(cotations_path)
            )
            
            cotations_count = df_cotations.count()
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
            
            # Convert to Spark DataFrame
            self.logger.info("🔄 Converting to Spark DataFrame")
            df_indices = self.spark.createDataFrame(indices_pdf, schema=self.indices_schema)
            
            # Add metadata columns
            self.logger.debug("➕ Adding metadata columns")
            df_indices = df_indices.withColumn(
                "ingestion_timestamp", current_timestamp()
            ).withColumn(
                "source_file", lit(indices_path)
            )
            
            indices_count = df_indices.count()
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

            # Save to Parquet
            saving_start = datetime.now()
            self.logger.info(f"💿 Saving cotations to Parquet: {bronze_cotations_path}")
            df_cotations.write.mode("overwrite").parquet(bronze_cotations_path)
            
            self.logger.info(f"💿 Saving indices to Parquet: {bronze_indices_path}")
            df_indices.write.mode("overwrite").parquet(bronze_indices_path)
            
            # Save to PostgreSQL
            self.logger.info("🐘 Saving cotations to PostgreSQL")
            self._save_to_postgres(df_cotations, 'bronze_cotations')
            
            self.logger.info("🐘 Saving indices to PostgreSQL")
            self._save_to_postgres(df_indices, 'bronze_indices')
            
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

            return df_cotations, df_indices

        except Exception as e:
            self.logger.error("❌ Bronze Layer Data Load failed", exc_info=True)
            self.logger.error(f"📉 Partial statistics:\n{json.dumps(loading_stats, indent=2)}")
            raise

    @timed_operation("PostgreSQL Save")
    def _save_to_postgres(self, df: DataFrame, table_name: str):
        """Save DataFrame to PostgreSQL with detailed logging"""
        db_logger = logging.getLogger('BronzeLevelLoader.postgres')
        db_logger.handlers = self.logger.handlers
       
        try:
            start_time = datetime.now()
            row_count = df.count()
            
            db_logger.info(f"💾 Saving {row_count:,} rows to PostgreSQL table '{table_name}'")
            
            # Construct proper JDBC URL
            jdbc_url = (
                f"jdbc:postgresql://{self.db_connector.db_config['host']}:"
                f"{self.db_connector.db_config['port']}/"
                f"{self.db_connector.db_config['database']}"
            )
            
            # Save to PostgreSQL
            df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", self.db_connector.db_config['username']) \
                .option("password", self.db_connector.db_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .save()
            
            duration = (datetime.now() - start_time).total_seconds()
            rows_per_second = int(row_count / duration) if duration > 0 else 0
            
            db_logger.info(
                f"✅ Saved to PostgreSQL:\n"
                f"  • Rows: {row_count:,}\n"
                f"  • Duration: {duration:.2f}s\n"
                f"  • Throughput: {rows_per_second:,} rows/s"
            )
       
        except Exception as e:
            db_logger.error(f"❌ Failed to save to PostgreSQL table '{table_name}'", exc_info=True)
            raise

    @timed_operation("Year Data Extraction")
    def extract_year_data(self, year: int):
        """
        Extract data for a specific year with detailed logging
        
        Args:
            year (int): Year to extract
            
        Returns:
            Tuple of DataFrames for cotations and indices of the specified year
        """
        extract_logger = logging.getLogger('BronzeLevelLoader.extract')
        extract_logger.handlers = self.logger.handlers
        
        try:
            start_time = datetime.now()
            extract_logger.info(f"📅 Extracting data for year {year}")
            
            # Filter Cotations by year
            cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            extract_logger.info(f"📂 Reading cotations from: {cotations_path}")
            
            df_cotations = self.spark.read.parquet(cotations_path)
            extract_logger.debug(f"📊 Total cotations rows: {df_cotations.count():,}")
            
            df_cotations_year = df_cotations.filter(col('annee') == year)
            cotations_count = df_cotations_year.count()
            extract_logger.info(f"🔍 Filtered to {cotations_count:,} rows for year {year}")
            
            # Filter Indices by year
            indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')
            extract_logger.info(f"📂 Reading indices from: {indices_path}")
            
            df_indices = self.spark.read.parquet(indices_path)
            extract_logger.debug(f"📊 Total indices rows: {df_indices.count():,}")
            
            df_indices_year = df_indices.filter(col('annee') == year)
            indices_count = df_indices_year.count()
            extract_logger.info(f"🔍 Filtered to {indices_count:,} rows for year {year}")

            # Log sample data
            if cotations_count > 0:
                extract_logger.debug(f"🔍 Cotations sample for {year}:\n{df_cotations_year.limit(2).toPandas().to_string()}")
            
            if indices_count > 0:
                extract_logger.debug(f"🔍 Indices sample for {year}:\n{df_indices_year.limit(2).toPandas().to_string()}")
            
            duration = (datetime.now() - start_time).total_seconds()
            extract_logger.info(
                f"✅ Extraction complete:\n"
                f"  • Duration: {duration:.2f}s\n"
                f"  • Results: {cotations_count:,} cotations rows, {indices_count:,} indices rows"
            )

            return df_cotations_year, df_indices_year

        except Exception as e:
            extract_logger.error(f"❌ Failed to extract data for year {year}", exc_info=True)
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