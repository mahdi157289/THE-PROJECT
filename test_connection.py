# test_connection.py
import sys
import os

# Get the absolute path of the project root
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Now add the parent directory to ensure imports work
parent_dir = os.path.dirname(project_root)
sys.path.insert(0, parent_dir)

# Debug print to verify paths
print("Project Root:", project_root)
print("Parent Directory:", parent_dir)
print("Python Path:", sys.path)

try:
    from config.settings import DB_CONFIG
    from src.utils.db_connection import DatabaseConnector
    from src.utils.error_logging import etl_logger

    def test_database_connection():
        """
        Comprehensive database connection test
        """
        print("\n===== Database Connection Diagnostic =====")
        
        try:
            # Print connection details (safely)
            print("Database Connection Details:")
            print(f"Host: {DB_CONFIG['host']}")
            print(f"Port: {DB_CONFIG['port']}")
            print(f"Database: {DB_CONFIG['database']}")
            print(f"Username: {DB_CONFIG['username']}")

            # Create DatabaseConnector instance
            db_connector = DatabaseConnector()

            # Test basic connection
            connection_status = db_connector.test_connection()
            print("\n1. Connection Test:")
            print("Status:", "✅ Successful" if connection_status else "❌ Failed")

            if not connection_status:
                print("Connection failed. Please check your database configuration.")
                return

            # Test query execution
            print("\n2. Query Execution Test:")
            try:
                # Simple test query
                result = db_connector.execute_query("SELECT current_timestamp")
                print("Current Database Time:", result.scalar())
            except Exception as query_error:
                print("❌ Query Execution Failed:", str(query_error))

            # Test table creation
            print("\n3. Table Creation Test:")
            try:
                db_connector.create_tables()
                print("✅ Tables created successfully")
            except Exception as table_error:
                print("❌ Table Creation Failed:", str(table_error))

            # Logging test
            etl_logger.info("Database connection test completed successfully")
            print("\n===== Connection Test Completed Successfully =====")

        except Exception as e:
            print("❌ Comprehensive Test Failed:", str(e))
            import traceback
            traceback.print_exc()

    def main():
        test_database_connection()

    if __name__ == "__main__":
        main()

except Exception as import_error:
    print("Import Error:", import_error)
    import traceback
    traceback.print_exc()