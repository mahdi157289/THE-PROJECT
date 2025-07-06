import logging
import sys
import os
from datetime import datetime
from src.utils.spark_session import SparkSessionManager

# Configure advanced logging with custom formatter
class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors and better formatting to log output"""
    COLORS = {
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[91m\033[1m',  # Bold Red
        'RESET': '\033[0m'      # Reset to default
    }
    
    def format(self, record):
        log_message = super().format(record)
        if record.levelname in self.COLORS:
            return f"{self.COLORS[record.levelname]}{log_message}{self.COLORS['RESET']}"
        return log_message

# Set up logging with enhanced formatting
def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)
    
    # Console handler with color formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = ColoredFormatter(
        '%(asctime)s │ %(levelname)-8s │ %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler for persistent logs
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(f"{log_dir}/spark_test_{timestamp}.log")
    file_formatter = logging.Formatter(
        '%(asctime)s │ %(levelname)-8s │ %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Suppress excessively verbose logging from other libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    
    return root_logger

def separator(char="─", width=80):
    """Print a separator line with optional title"""
    logging.info(char * width)

def section_header(title):
    """Print a section header with title"""
    width = 80
    separator("═", width)
    padding = (width - len(title) - 4) // 2
    logging.info(f"{'═' * padding} {title} {'═' * (width - padding - len(title) - 4)}")
    separator("═", width)

def test_spark_creation():
    """Test creating a Spark session and verify version"""
    section_header("TESTING SPARK SESSION CREATION")
    
    try:
        spark_manager = SparkSessionManager()
        spark = spark_manager.get_spark_session()
        
        # Get versions
        spark_version = spark.version
        hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        
        logging.info(f"✓ Spark session created successfully")
        logging.info(f"┌─ Spark version: {spark_version}")
        logging.info(f"└─ Hadoop version: {hadoop_version}")
        
        # Test singleton pattern
        second_manager = SparkSessionManager()
        is_same_instance = spark_manager is second_manager
        logging.info(f"✓ Singleton pattern {'working correctly' if is_same_instance else 'FAILED'}")
        
        return spark_manager
    except Exception as e:
        logging.error(f"✗ Failed to create Spark session: {e}")
        raise

def test_csv_reading(spark_manager, csv_path):
    """Test reading a CSV file"""
    section_header("TESTING CSV READING")
    
    try:
        # Read CSV file
        df = spark_manager.read_csv(csv_path)
        row_count = df.count()
        col_count = len(df.columns)
        
        # Show basic information
        logging.info(f"✓ Successfully read CSV from {csv_path}")
        logging.info(f"┌─ Number of rows: {row_count}")
        logging.info(f"└─ Number of columns: {col_count}")
        
        # Show schema
        logging.info("Schema:")
        for field in df.schema.fields:
            logging.info(f"  ├─ {field.name}: {field.dataType}")
            
        # Show sample data (first 3 rows only)
        logging.info("Sample data (first 3 rows):")
        for row in df.limit(3).collect():
            logging.info(f"  ├─ {row}")
            
        return df
    except Exception as e:
        logging.error(f"✗ Failed to read CSV: {e}")
        raise

def test_metadata_columns(spark_manager, df):
    """Test adding metadata columns"""
    section_header("TESTING METADATA COLUMNS")
    
    try:
        df_with_metadata = spark_manager.add_metadata_columns(df)
        
        # Verify metadata columns exist
        has_timestamp = "ingestion_timestamp" in df_with_metadata.columns
        has_source = "source_file" in df_with_metadata.columns
        
        if has_timestamp and has_source:
            logging.info(f"✓ Successfully added metadata columns")
            
            # Show sample of metadata columns
            sample_row = df_with_metadata.select("ingestion_timestamp", "source_file").first()
            if sample_row:
                logging.info(f"┌─ Sample timestamp: {sample_row['ingestion_timestamp']}")
                logging.info(f"└─ Sample source file: {sample_row['source_file']}")
        else:
            missing = []
            if not has_timestamp:
                missing.append("ingestion_timestamp")
            if not has_source:
                missing.append("source_file")
            logging.warning(f"⚠ Missing metadata columns: {', '.join(missing)}")
        
        return df_with_metadata
    except Exception as e:
        logging.error(f"✗ Failed to add metadata columns: {e}")
        raise

def test_parquet_writing(spark_manager, df, output_path):
    """Test writing DataFrame to Parquet"""
    section_header("TESTING PARQUET WRITING")
    
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Remove existing output if any
        if os.path.exists(output_path):
            import shutil
            shutil.rmtree(output_path)
            logging.info(f"Removed existing output directory: {output_path}")
        
        # Write to Parquet
        spark_manager.write_parquet(df, output_path)
        
        # Verify the file was created
        if os.path.exists(output_path):
            # List files in the output directory
            files = os.listdir(output_path) if os.path.isdir(output_path) else [output_path]
            parquet_files = [f for f in files if f.endswith('.parquet') or f == '_SUCCESS']
            
            logging.info(f"✓ Successfully wrote DataFrame to {output_path}")
            logging.info(f"  └─ Created {len(parquet_files)} files")
            
            # Try reading it back to verify
            try:
                df_read = spark_manager.read_parquet(output_path)
                read_count = df_read.count()
                logging.info(f"✓ Successfully verified Parquet read with {read_count} rows")
            except Exception as e:
                logging.warning(f"⚠ Could not verify Parquet read: {e}")
        else:
            logging.warning(f"⚠ Output path not found: {output_path}")
        
        return True
    except Exception as e:
        logging.error(f"✗ Failed to write Parquet: {e}")
        raise

def test_error_handling(spark_manager):
    """Test error handling with invalid inputs"""
    section_header("TESTING ERROR HANDLING")
    
    # Test with non-existent file
    try:
        logging.info("Attempting to read non-existent file...")
        df = spark_manager.read_csv("non_existent_file.csv")
        logging.error("✗ Expected error not raised!")
    except Exception as e:
        logging.info(f"✓ Correctly caught error: {str(e)[:100]}...")
    
    # Test with malformed CSV
    try:
        # Create a temporary malformed CSV
        with open("temp_malformed.csv", "w") as f:
            f.write("header1,header2,header3\n")
            f.write("value1,value2\n")  # Missing a column
        
        logging.info("Attempting to read malformed CSV...")
        df = spark_manager.read_csv("temp_malformed.csv")
        
        # Should work but with warnings
        if df.count() > 0:
            logging.info("✓ Handled malformed CSV (Spark's default behavior)")
        
        # Clean up
        os.remove("temp_malformed.csv")
    except Exception as e:
        logging.info(f"Caught error with malformed CSV: {e}")
        # Clean up in case of exception
        if os.path.exists("temp_malformed.csv"):
            os.remove("temp_malformed.csv")

def main():
    """Main test function"""
    setup_logging()
    
    # Print start banner
    print("\n")
    separator("═", 80)
    logging.info(f"  SPARK SESSION MANAGER TEST SUITE")
    logging.info(f"  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    separator("═", 80)
    print("\n")
    
    # Paths for testing
    test_data_path = "data/sample.csv"  # Update this with your valid path
    parquet_output_path = "data/test_output.parquet"
    
    # Track test results
    results = {"passed": 0, "failed": 0, "warnings": 0}
    
    # Run tests with appropriate error handling
    try:
        # Test 1: Create Spark Session
        spark_manager = test_spark_creation()
        results["passed"] += 1
        
        # Test 2: Read CSV
        df = test_csv_reading(spark_manager, test_data_path)
        results["passed"] += 1
        
        # Test 3: Add Metadata Columns
        df_with_metadata = test_metadata_columns(spark_manager, df)
        results["passed"] += 1
        
        # Test 4: Write Parquet
        test_parquet_writing(spark_manager, df_with_metadata, parquet_output_path)
        results["passed"] += 1
        
        # Test 5: Error Handling
        test_error_handling(spark_manager)
        results["passed"] += 1
        
        # Shut down the Spark session
        section_header("CLEANUP")
        logging.info("Stopping Spark session...")
        spark_manager.close()
        logging.info("✓ Spark session stopped")
        
    except Exception as e:
        logging.error(f"Test suite error: {e}")
        results["failed"] += 1
    
    # Print summary
    print("\n")
    section_header("TEST SUMMARY")
    logging.info(f"Tests passed: {results['passed']}")
    logging.info(f"Tests failed: {results['failed']}")
    if results["warnings"] > 0:
        logging.info(f"Warnings: {results['warnings']}")
    
    overall_status = "SUCCESS" if results["failed"] == 0 else "FAILURE"
    separator("═", 80)
    logging.info(f"Overall status: {overall_status}")
    separator("═", 80)

if __name__ == "__main__":
    main()