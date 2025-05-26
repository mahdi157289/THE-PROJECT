import os
import logging
from datetime import datetime
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, col, lit

from src.utils.db_connection import DatabaseConnector
from src.bronze_layer.schemas import create_bronze_schemas
from config.settings import PATHS
from src.utils.spark_session import SparkSessionManager

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

    def _setup_logger(self):
        """
        Set up detailed logging configuration
        """
        logger = logging.getLogger('BronzeLevelLoader')
        logger.setLevel(logging.DEBUG)  # Changed to DEBUG for more detailed logging
        
        # Clear existing handlers to avoid duplicate logs
        if logger.hasHandlers():
            logger.handlers.clear()
        
        # File Handler
        file_handler = logging.FileHandler(os.path.join(PATHS['logs'], 'bronze_loader.log'))
        file_handler.setLevel(logging.DEBUG)
        
        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def _preprocess_data(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Preprocess input data with robust type handling
        
        Args:
            df (pd.DataFrame): Input DataFrame
            data_type (str): 'cotations' or 'indices'
        
        Returns:
            Preprocessed DataFrame
        """
        try:
            self.logger.debug(f"Starting preprocessing for {data_type} with columns: {df.columns.tolist()}")
            
            # Add year column
            df['annee'] = pd.to_datetime(df['seance']).dt.year
            
            # Additional preprocessing based on data type
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

            # Convert numeric columns safely
            for col_name in numeric_cols:
                if col_name in df.columns:
                    self.logger.debug(f"Processing column: {col_name}")
                    
                    # First ensure it's string type if it contains commas
                    if df[col_name].dtype == object:
                        df[col_name] = df[col_name].astype(str).str.replace(',', '.')
                    
                    # Then convert to numeric
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    
                    # Log conversion stats
                    num_null = df[col_name].isna().sum()
                    if num_null > 0:
                        self.logger.warning(f"Converted {num_null} null/NA values in column {col_name}")
            
            self.logger.debug(f"Preprocessing completed for {data_type}")
            return df
        
        except Exception as e:
            self.logger.error(f"Preprocessing error for {data_type}: {str(e)}", exc_info=True)
            raise

    def _read_csv_with_fallback(self, file_path: str, data_type: str) -> pd.DataFrame:
        """
        Read CSV with robust error handling and type inference
        
        Args:
            file_path (str): Path to CSV file
            data_type (str): 'cotations' or 'indices'
            
        Returns:
            pd.DataFrame: Loaded dataframe
        """
        try:
            self.logger.info(f"Attempting to read {data_type} from {file_path}")
            
            # First try with automatic type inference
            try:
                df = pd.read_csv(file_path, parse_dates=['SEANCE' if data_type == 'cotations' else 'seance'])
                self.logger.debug("Successfully read with automatic type inference")
                return df
            except Exception as e:
                self.logger.warning(f"Automatic type inference failed, trying with low_memory=False: {str(e)}")
                df = pd.read_csv(file_path, parse_dates=['SEANCE' if data_type == 'cotations' else 'seance'], low_memory=False)
                self.logger.debug("Successfully read with low_memory=False")
                return df
                
        except Exception as e:
            self.logger.error(f"Failed to read {data_type} CSV: {str(e)}", exc_info=True)
            raise

    def load_bronze_data(self):
        """
        Load data to Bronze layer with comprehensive error handling
        """
        start_time = datetime.now()
        self.logger.info("Starting Bronze Layer Data Load")
        self.logger.debug(f"PATHS configuration: {PATHS}")

        try:
            # Load Cotations
            cotations_path = os.path.join(PATHS['input_data'], 'cotations', 'cotations.csv')
            self.logger.info(f"Loading Cotations from {cotations_path}")
            
            # Read CSV with robust handling
            cotations_pdf = self._read_csv_with_fallback(cotations_path, 'cotations')
            cotations_pdf.columns = cotations_pdf.columns.str.lower()
            self.logger.debug(f"Cotations raw data sample:\n{cotations_pdf.head(2).to_string()}")
            
            # Preprocess
            cotations_pdf = self._preprocess_data(cotations_pdf, 'cotations')
            
            # Convert to Spark DataFrame
            df_cotations = self.spark.createDataFrame(cotations_pdf, schema=self.cotations_schema)
            
            # Add metadata columns
            df_cotations = df_cotations.withColumn(
                "ingestion_timestamp", current_timestamp()
            ).withColumn(
                "source_file", lit(cotations_path)  # Using lit() since we're not reading directly from file
            )

            # Load Indices
            indices_path = os.path.join(PATHS['input_data'], 'indices', 'indices.csv')
            self.logger.info(f"Loading Indices from {indices_path}")
            
            # Read CSV with robust handling
            indices_pdf = self._read_csv_with_fallback(indices_path, 'indices')
            indices_pdf.columns = indices_pdf.columns.str.lower()
            self.logger.debug(f"Indices raw data sample:\n{indices_pdf.head(2).to_string()}")
            
            # Preprocess
            indices_pdf = self._preprocess_data(indices_pdf, 'indices')
            
            # Convert to Spark DataFrame
            df_indices = self.spark.createDataFrame(indices_pdf, schema=self.indices_schema)
            
            # Add metadata columns
            df_indices = df_indices.withColumn(
                "ingestion_timestamp", current_timestamp()
            ).withColumn(
                "source_file", lit(indices_path)  # Using lit() since we're not reading directly from file
            )

            # Log loading statistics
            self.logger.info(f"Cotations Rows: {df_cotations.count()}")
            self.logger.info(f"Indices Rows: {df_indices.count()}")
            self.logger.debug(f"Cotations Schema: {df_cotations.schema}")
            self.logger.debug(f"Indices Schema: {df_indices.schema}")

            # Save to Bronze Layer (Parquet)
            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')

            self.logger.info(f"Saving cotations to {bronze_cotations_path}")
            df_cotations.write.mode("overwrite").parquet(bronze_cotations_path)
            
            self.logger.info(f"Saving indices to {bronze_indices_path}")
            df_indices.write.mode("overwrite").parquet(bronze_indices_path)

            # Save to PostgreSQL
            self._save_to_postgres(df_cotations, 'bronze_cotations')
            self._save_to_postgres(df_indices, 'bronze_indices')

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"Bronze Layer Data Load completed successfully in {duration:.2f} seconds")

            return df_cotations, df_indices

        except Exception as e:
            self.logger.error(f"Bronze Layer Data Load failed: {str(e)}", exc_info=True)
            raise

    def _save_to_postgres(self, df: DataFrame, table_name: str):
        """
        Save DataFrame to PostgreSQL with detailed logging
        
        Args:
            df (DataFrame): Spark DataFrame to save
            table_name (str): Target table name
        """
        try:
            self.logger.info(f"Attempting to save {table_name} to PostgreSQL")
            self.logger.debug(f"DataFrame schema for {table_name}:\n{df.schema}")
            
            df.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", self.db_connector.connection_string) \
                .option("dbtable", table_name) \
                .option("user", self.db_connector.db_config['username']) \
                .option("password", self.db_connector.db_config['password']) \
                .option("driver", "org.postgresql.Driver") \
                .save()
            
            self.logger.info(f"Successfully saved {table_name} to PostgreSQL")
        
        except Exception as e:
            self.logger.error(f"Failed to save {table_name} to PostgreSQL: {str(e)}", exc_info=True)
            raise

    def extract_year_data(self, year: int):
        """
        Extract data for a specific year with detailed logging
        
        Args:
            year (int): Year to extract
            
        Returns:
            Tuple of DataFrames for cotations and indices of the specified year
        """
        try:
            self.logger.info(f"Extracting data for year {year}")
            
            # Filter Cotations by year
            cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            self.logger.debug(f"Reading cotations from {cotations_path}")
            df_cotations = self.spark.read.parquet(cotations_path).filter(col('annee') == year)
            
            # Filter Indices by year
            indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')
            self.logger.debug(f"Reading indices from {indices_path}")
            df_indices = self.spark.read.parquet(indices_path).filter(col('annee') == year)

            self.logger.info(f"Extracted data for year {year}")
            self.logger.info(f"Cotations Rows: {df_cotations.count()}")
            self.logger.info(f"Indices Rows: {df_indices.count()}")
            self.logger.debug(f"Cotations sample for {year}:\n{df_cotations.limit(2).toPandas().to_string()}")
            self.logger.debug(f"Indices sample for {year}:\n{df_indices.limit(2).toPandas().to_string()}")

            return df_cotations, df_indices

        except Exception as e:
            self.logger.error(f"Failed to extract data for year {year}: {str(e)}", exc_info=True)
            raise

def main():
    try:
        loader = BronzeLevelLoader()
        
        # Load full dataset to Bronze layer
        loader.load_bronze_data()
        
        # Optional: Extract data for a specific year
        # cotations_2020, indices_2020 = loader.extract_year_data(2020)
        
    except Exception as e:
        logging.getLogger('BronzeLevelLoader').critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()