import os
import sys
import logging
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.utils.spark_session import SparkSessionManager

class TestSparkSession(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize Spark session for all tests"""
        cls.spark_manager = SparkSessionManager()
        cls.spark = cls.spark_manager.get_spark_session()
        cls.logger = logging.getLogger('SparkSessionTest')
        cls.logger.setLevel(logging.INFO)
        
        # Add console handler if not exists
        if not cls.logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            cls.logger.addHandler(ch)

    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests"""
        cls.spark_manager.close()
        cls.logger.info("Spark session stopped")

    def test_01_spark_session_creation(self):
        """Test if Spark session is created successfully"""
        self.assertIsInstance(self.spark, SparkSession)
        self.logger.info("Spark session created successfully")
        
        # Verify Spark UI is accessible
        ui_url = self.spark.sparkContext.uiWebUrl
        self.assertTrue(ui_url.startswith('http://'))
        self.logger.info(f"Spark UI available at: {ui_url}")

    def test_02_ivy_configuration(self):
        """Test Ivy dependency resolution configuration"""
        ivy_path = self.spark.conf.get("spark.jars.ivy")
        self.assertTrue(ivy_path.startswith('/opt/bitnami/spark/.ivy2'))
        self.logger.info(f"Ivy path configured correctly: {ivy_path}")
        
        # Test package resolution
        test_df = self.spark.createDataFrame([("test",)], ["dummy"])
        test_df.write.mode("overwrite").parquet("/tmp/spark_test_parquet")
        self.logger.info("Basic file operations working")

    def test_03_network_connectivity(self):
        """Test network connectivity between driver and executors"""
        from socket import gethostbyname, gaierror
        try:
            # Test master host resolution
            master_host = "spark-master"
            gethostbyname(master_host)
            self.logger.info(f"Host resolution working for {master_host}")
        except gaierror:
            self.fail(f"Could not resolve {master_host}")

    def test_04_resource_allocation(self):
        """Test resource allocation and configuration"""
        executor_mem = self.spark.conf.get("spark.executor.memory")
        self.assertEqual(executor_mem, "4g")
        driver_mem = self.spark.conf.get("spark.driver.memory")
        self.assertEqual(driver_mem, "2g")
        self.logger.info(f"Resource allocation correct - Executor: {executor_mem}, Driver: {driver_mem}")

    def test_05_dataframe_operations(self):
        """Test basic DataFrame operations"""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True)
        ])
        
        test_data = [(1, "Alice"), (2, "Bob"), (3, None)]
        df = self.spark.createDataFrame(test_data, schema)
        
        # Test transformations
        df_filtered = df.filter(df.id > 1)
        self.assertEqual(df_filtered.count(), 2)
        
        # Test null handling
        null_count = df.filter(df.name.isNull()).count()
        self.assertEqual(null_count, 1)
        
        self.logger.info("Basic DataFrame operations working correctly")

    def test_06_write_read_parquet(self):
        """Test Parquet read/write operations"""
        test_path = "/tmp/spark_test_parquet"
        test_data = [(1, "Paris"), (2, "London")]
        df = self.spark.createDataFrame(test_data, ["id", "city"])
        
        # Write test
        df.write.mode("overwrite").parquet(test_path)
        self.assertTrue(len(os.listdir(test_path)) > 0)
        self.logger.info("Parquet write successful")
        
        # Read test
        df_read = self.spark.read.parquet(test_path)
        self.assertEqual(df_read.count(), 2)
        self.logger.info("Parquet read successful")

    def test_07_postgres_connectivity(self):
        """Test PostgreSQL JDBC connectivity"""
        try:
            from src.utils.db_connection import DatabaseConnector
            db = DatabaseConnector()
            connection = db.get_connection()
            self.assertTrue(connection.is_valid())
            self.logger.info("PostgreSQL connection successful")
            connection.close()
        except Exception as e:
            self.logger.warning(f"PostgreSQL test skipped - no valid connection: {str(e)}")
            raise unittest.SkipTest("PostgreSQL connection not available")

    def test_08_cluster_connectivity(self):
        """Test if worker nodes are properly connected"""
        if self.spark.conf.get("spark.master").startswith("spark://"):
            try:
                status = self.spark._jsc.sc().getExecutorMemoryStatus()
                self.assertGreater(len(status), 1)  # At least 1 executor
                self.logger.info(f"Cluster connectivity OK - {len(status)} executors connected")
            except Exception as e:
                self.fail(f"Cluster connectivity failed: {str(e)}")
        else:
            self.logger.info("Local mode - skipping cluster connectivity test")
            raise unittest.SkipTest("Not running in cluster mode")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("spark_session_test.log"),
            logging.StreamHandler()
        ]
    )
    
    # Run tests
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
    
    # Manual validation for Windows-specific issues
    if os.name == 'nt':
        print("\nWindows Host Validation:")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Path separator: {os.path.sep}")
        print("Verify all paths in Spark UI use forward slashes (/)")