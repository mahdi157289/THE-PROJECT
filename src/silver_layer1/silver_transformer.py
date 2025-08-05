import os
import sys
import logging
import pandas as pd
from typing import Tuple
from config.settings import PATHS
from src.utils.db_connection import DatabaseConnector
from .data_cleaner import DataCleaner
from .data_transformer import DataTransformer
from .data_saver import DataSaver
from .schemas import get_silver_cotations_schema, get_silver_indices_schema, get_rejected_schema, get_rejected_prime_schema
from .models import RejectedRecord

class SilverLevelTransformer:
    def __init__(self):
        self.logger = self._setup_logger()
        self.db_connector = DatabaseConnector()
        self.engine = self.db_connector.engine
        
        # Initialize components
        self.cleaner = DataCleaner(self.logger)
        self.transformer = DataTransformer(self.logger)
        self.saver = DataSaver(self.engine, self.logger)
        
        # Load schemas
        self.silver_cotations_schema = get_silver_cotations_schema()
        self.silver_indices_schema = get_silver_indices_schema()
        self.rejected_schema = get_rejected_schema()
        self.rejected_prime_schema = get_rejected_prime_schema()

    def _setup_logger(self):
        logger = logging.getLogger('SilverTransformer')
        logger.setLevel(logging.DEBUG)  # Changed from INFO to DEBUG for more granular logs
        
        # Clear existing handlers if any
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # File handler with detailed format
        file_handler = logging.FileHandler(
            os.path.join(PATHS['logs'], 'silver_transformer.log'),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-25s | %(module)-15s | %(funcName)-20s | %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler with simpler format
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(processName)-12s | %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        # Add exception logging
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            
            logger.critical(
                "Uncaught exception",
                exc_info=(exc_type, exc_value, exc_traceback)
            )
        
        sys.excepthook = handle_exception
        
        return logger

    # silver_transformer.py (updated _ensure_schemas_exist method)
    def _ensure_schemas_exist(self):
        """Execute all DDL statements to create required tables with ALTER safeguards"""
        try:
            self.logger.info("🏗️ Ensuring database schemas exist")
             # First drop the table if it exists to ensure clean slate
            self.saver.execute_ddl("DROP TABLE IF EXISTS rejected_prime")
            self.saver.execute_ddl("DROP TABLE IF EXISTS rejected")
            self.saver.execute_ddl("DROP TABLE IF EXISTS silver_indices")
            self.saver.execute_ddl("DROP TABLE IF EXISTS silver_cotations")
            # Execute initial DDLs
            self.saver.execute_ddl(self.silver_cotations_schema)
            self.saver.execute_ddl(self.silver_indices_schema)
            self.saver.execute_ddl(self.rejected_schema)
            self.saver.execute_ddl(self.rejected_prime_schema)
            
            # Add missing columns to rejected_cotations
            alter_queries = [
                "ALTER TABLE rejected_cotations ADD COLUMN IF NOT EXISTS annee INTEGER",
                "ALTER TABLE rejected_prime ADD COLUMN IF NOT EXISTS variation_pourcentage DOUBLE PRECISION",
                "ALTER TABLE rejected_prime ADD COLUMN IF NOT EXISTS variation_indice DOUBLE PRECISION",
                # Add other potentially missing columns here as needed
            ]
            
            for query in alter_queries:
                try:
                    self.saver.execute_ddl(query)
                except Exception as alter_error:
                    self.logger.warning(f"Schema alteration warning: {str(alter_error)}")
            
            self.logger.info("✅ Database schemas verified/created")
        except Exception as e:
            self.logger.error("Failed to initialize database schemas", exc_info=True)
            raise

    def transform_bronze_to_silver(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Main transformation pipeline with enhanced logging"""
        try:
            self.logger.info("🚀 Starting Bronze to Silver transformation")
            
            # First ensure all tables exist
            self._ensure_schemas_exist()
            
            # Read source data
            self.logger.debug("Loading bronze data from storage")
            df_cotations, df_indices = self._load_bronze_data()
            self.logger.info(f"📥 Loaded {len(df_cotations):,} cotations and {len(df_indices):,} indices")
            
            # Clean and transform cotations
            self.logger.info("🧹 Starting cotation data cleaning")
            clean_cotations, rejected = self.cleaner.clean_data(df_cotations, 'bronze_cotations.parquet')
            
            if not rejected.empty:
                # Save only first 10 rejected records
                rejected_sample = rejected.head(10)
                self.logger.info(f"❌ Saving sample of {len(rejected_sample):,} rejected records (initial cleaning)")
                self.saver.save_to_postgres(rejected_sample, 'rejected_cotations')
            
            self.logger.info("⚙️ Transforming cleaned cotations")
            silver_cotations, rejected_prime_cotations = self.transformer.transform_cotations(
                clean_cotations, 'bronze_cotations.parquet'
            )
            self.logger.info(f"✅ Transformed {len(silver_cotations):,} cotations, rejected {len(rejected_prime_cotations):,}")
            
            # Clean and transform indices
            self.logger.info("🧹 Starting indices data cleaning")
            clean_indices, _ = self.cleaner.clean_indices(df_indices, 'bronze_indices.parquet')
            
            self.logger.info("📈 Starting indices transformation")
            silver_indices, rejected_prime_indices = self.transformer.transform_indices(
                clean_indices, 'bronze_indices.parquet'  # Use cleaned data
            )
            # Combine and save first 10 rejected prime records
            all_rejected_prime = pd.concat([rejected_prime_cotations, rejected_prime_indices])
            if not all_rejected_prime.empty:
                # Save only first 10 rejected prime records
                rejected_prime_sample = all_rejected_prime.head(10)
                self.logger.info(f"❌ Saving sample of {len(rejected_prime_sample):,} NULL-rejected records")
                self.saver.save_to_postgres(rejected_prime_sample, 'rejected_prime')
            
            # Save silver data
            self.logger.info("💾 Saving silver data to storage")
            self._save_silver_data(silver_cotations, silver_indices)
            
            self.logger.info(f"🎉 Successfully processed {len(silver_cotations):,} cotations and {len(silver_indices):,} indices")
            return silver_cotations, silver_indices
                
        except Exception as e:
            self.logger.critical("🔥 Transformation failed", exc_info=True)
            raise

    def _load_bronze_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load bronze layer data"""
        bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
        bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')
        
        df_cotations = pd.read_parquet(bronze_cotations_path)
        df_indices = pd.read_parquet(bronze_indices_path)
        
        self.logger.info(f"Loaded {len(df_cotations)} cotations and {len(df_indices)} indices")
        return df_cotations, df_indices

    def _save_silver_data(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> None:
        """Save silver data to storage"""
        # Save to Parquet
        cotations_path = os.path.join(PATHS['silver_data'], 'silver_cotations.parquet')
        indices_path = os.path.join(PATHS['silver_data'], 'silver_indices.parquet')
        
        self.saver.save_to_parquet(cotations, cotations_path)
        self.saver.save_to_parquet(indices, indices_path)
        
        # Save to PostgreSQL
        self.saver.save_to_postgres(cotations, 'silver_cotations')
        self.saver.save_to_postgres(indices, 'silver_indices')

def main():
    transformer = SilverLevelTransformer()
    try:
        transformer.transform_bronze_to_silver()
        return 0
    except Exception as e:
        transformer.logger.error("Pipeline execution failed", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())