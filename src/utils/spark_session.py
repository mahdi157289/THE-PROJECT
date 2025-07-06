import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name  # Added import for input_file_name
from config.settings import SPARK_CONFIG, PATHS
import logging

class SparkSessionManager:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._create_spark_session()
        return cls._instance

    def _create_spark_session(self):
        """
        Create a configured Spark Session
        """
        try:
            self.spark = (
            SparkSession.builder
            .appName(SPARK_CONFIG['app_name'])
            .master(SPARK_CONFIG['master'])
            # Add your existing configurations
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
           
            
            
            # ... Spark configs ...
            .config("spark.executor.memory", "16g")  # Increased memory
            .config("spark.driver.memory", "16g")     # Increased memory
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.network.timeout", "6000s")  # Increased timeout
            .config("spark.executor.heartbeatInterval", "120s")  # Increased heartbeat interval
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")   # For timestamp handling
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
            .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
            # hadoop configurations for Windows
            .config("spark.hadoop.home.dir", "C:/Users/bacca/Downloads/winutils/hadoop-3.3.5")
            # Add the workaround configurations
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.parquet.summary.metadata.level", "none")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
      
            # Additional helpful config for Windows
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.rpc.message.maxSize", "512")  # Maximum size (in MB) of messages that can be exchanged between Spark processes. Default is 128MB
            .config("spark.core.connection.ack.wait.timeout", "600s")  # How long to wait for a connection to be acknowledged. Default is 60s
            .config("spark.shuffle.io.connectionTimeout", "600s")  # Default is 120s
            .getOrCreate()
           )
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logging.info("Spark Session created successfully")
        
        except Exception as e:
            logging.error(f"Failed to create Spark Session: {e}")
            raise

    def get_spark_session(self):
        """
        Get the existing Spark Session
        """
        return self.spark

    def read_csv(
        self, 
        path, 
        header=True, 
        infer_schema=True, 
        delimiter=',',
        **kwargs
    ):
        """
        Read CSV with flexible configuration
        """
        try:
            df = (
                self.spark.read
                .format("csv")
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .options(**kwargs)
                .load(path)
            )
            return df
        except Exception as e:
            logging.error(f"Error reading CSV from {path}: {e}")
            raise

    def read_parquet(self, path):
        """
        Read Parquet file
        """
        try:
            return self.spark.read.parquet(path)
        except Exception as e:
            logging.error(f"Error reading Parquet from {path}: {e}")
            raise

    def write_parquet(self, df, path, mode='overwrite'):
        """
        Write DataFrame to Parquet
        """
        try:
            df.write.mode(mode).parquet(path)
            logging.info(f"DataFrame written to {path}")
        except Exception as e:
            logging.error(f"Error writing Parquet to {path}: {e}")
            raise

    def add_metadata_columns(self, df):
        """
        Add standard metadata columns to DataFrame
        """
        try:
            # Using input_file_name() as a function, not a column reference
            return (
                df.withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file", input_file_name())  # Corrected line
            )
        except Exception as e:
            logging.error(f"Error adding metadata columns: {e}")
            # Provide a fallback for cases where input_file_name isn't available
            return df.withColumn("ingestion_timestamp", current_timestamp())

    def close(self):
        """
        Close Spark Session
        """
        if self.spark:
            self.spark.stop()
            logging.info("Spark Session stopped")