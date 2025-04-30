# test_spark_session.py
import logging
from src.utils.spark_session import SparkSessionManager

# Configure logging to output to console
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        logging.info("Starting SparkSession test...")
        
        # Create a SparkSessionManager instance
        spark_manager = SparkSessionManager()
        spark = spark_manager.get_spark_session()
        
        # Verify Spark session creation
        logging.info(f"Spark version: {spark.version}")
        
        # Test reading a CSV file
        logging.info("Testing read_csv method...")
        test_data_path = "data/sample.csv"  # Update this with a valid path
        df = spark_manager.read_csv(test_data_path)
        df.show(5)  # Display first 5 rows
        
        logging.info("read_csv method works successfully.")
        
        # Test adding metadata columns
        logging.info("Testing add_metadata_columns method...")
        df_with_metadata = spark_manager.add_metadata_columns(df)
        df_with_metadata.show(5)
        
        logging.info("add_metadata_columns method works successfully.")
        
        # Test writing to Parquet
        logging.info("Testing write_parquet method...")
        parquet_path = "data/sample_output.parquet"  # Update this with your desired path
        spark_manager.write_parquet(df_with_metadata, parquet_path)
        
        logging.info("write_parquet method works successfully.")
        
        # Stop Spark session
        spark_manager.close()
        logging.info("SparkSession test completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during testing: {e}")

if __name__ == "__main__":
    main()
