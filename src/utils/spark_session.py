# src/utils/spark_session.py
import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
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
                # Add PostgreSQL connector
                .config("spark.jars.packages", 
                    "org.postgresql:postgresql:42.5.4")
                # Memory and executor configurations
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", "200")
                # Enable Arrow-based conversion
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
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
        return (
            df.withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("input_file_name"))
        )

    def close(self):
        """
        Close Spark Session
        """
        if self.spark:
            self.spark.stop()
            logging.info("Spark Session stopped")
            
            
            
            
