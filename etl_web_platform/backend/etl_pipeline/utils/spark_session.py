import os
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
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
        """Create a configured Spark Session with all original configurations"""
        try:
            # Windows-specific configurations
            if os.name == 'nt':
                hadoop_home = "C:/Users/bacca/Downloads/winutils/hadoop-3.3.5"
                os.environ['HADOOP_HOME'] = hadoop_home
                os.environ['PATH'] = f"{hadoop_home}/bin;{os.environ['PATH']}"
                
            # Ivy path handling
            ivy_path = "/opt/bitnami/spark/.ivy2"
            ivy_settings_path = "/opt/bitnami/spark/conf/ivy.xml"
            if os.name == 'nt':
                ivy_path = ivy_path.replace("\\", "/")

            builder = (
                SparkSession.builder
                .appName(SPARK_CONFIG['app_name'])
                .master(SPARK_CONFIG['master'])
                
                # Ivy configuration (preserved)
                .config("spark.jars.ivy", ivy_path)
                .config("spark.jars.ivySettings", ivy_settings_path)
                .config("spark.jars.repositories", "https://repo1.maven.org/maven2/")
                
                # Memory and cores (preserved)
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.cores", "2")
                .config("spark.executor.instances", "2")
                
                # Networking (preserved)
                .config("spark.driver.host", "host.docker.internal")
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.network.timeout", "6000s")
                .config("spark.executor.heartbeatInterval", "120s")
                
                # Arrow configuration (preserved)
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                
                # Serialization (preserved)
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                
                # File handling (preserved)
                .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
                .config("spark.sql.sources.commitProtocolClass", 
                      "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
                .config("spark.sql.parquet.output.committer.class", 
                      "org.apache.parquet.hadoop.ParquetOutputCommitter")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.hadoop.parquet.summary.metadata.level", "none")
                
                # Windows-specific (preserved)
                .config("spark.hadoop.home.dir", "C:/Users/bacca/Downloads/winutils/hadoop-3.3.5")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                
                # Additional performance (preserved)
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.default.parallelism", "200")
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.3")
                .config("spark.rpc.message.maxSize", "512")
                .config("spark.core.connection.ack.wait.timeout", "600s")
                .config("spark.shuffle.io.connectionTimeout", "600s")
                .config("spark.port.maxRetries", "100")
                .config("spark.ui.port", "4041")
                
                # Reflection (preserved)
                .config("spark.driver.extraJavaOptions", 
                      "-Dio.netty.tryReflectionSetAccessible=true -Djava.net.preferIPv4Stack=true")
                .config("spark.executor.extraJavaOptions", 
                      "-Dio.netty.tryReflectionSetAccessible=true -Djava.net.preferIPv4Stack=true")
            )
            
            self.spark = builder.getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            logging.info("Spark Session created successfully with all configurations")
            logging.info(f"Spark Version: {self.spark.version}")
            logging.info(f"Java Version: {self.spark._jvm.java.lang.System.getProperty('java.version')}")
            
        except Exception as e:
            logging.error(f"Failed to create Spark Session: {e}")
            raise

    # [Rest of your methods remain exactly the same...]
    def get_spark_session(self):
        return self.spark

    def read_csv(self, path, header=True, infer_schema=True, delimiter=',', **kwargs):
        try:
            return (
                self.spark.read
                .option("header", header)
                .option("inferSchema", infer_schema)
                .option("delimiter", delimiter)
                .options(**kwargs)
                .csv(str(path))
            )
        except Exception as e:
            logging.error(f"Error reading CSV from {path}: {e}")
            raise

    def read_parquet(self, path):
        try:
            return self.spark.read.parquet(str(path))
        except Exception as e:
            logging.error(f"Error reading Parquet from {path}: {e}")
            raise

    def write_parquet(self, df, path, mode='overwrite', partition_by=None):
        try:
            writer = df.write.mode(mode)
            if partition_by:
                writer = writer.partitionBy(partition_by)
            writer.parquet(str(path))
            logging.info(f"DataFrame written to {path}")
        except Exception as e:
            logging.error(f"Error writing Parquet to {path}: {e}")
            raise

    def add_metadata_columns(self, df):
        try:
            return (
                df.withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file", input_file_name())
            )
        except Exception as e:
            logging.error(f"Error adding metadata columns: {e}")
            return df.withColumn("ingestion_timestamp", current_timestamp())

    def close(self):
        if self.spark:
            self.spark.stop()
            logging.info("Spark Session stopped")
            self._instance = None