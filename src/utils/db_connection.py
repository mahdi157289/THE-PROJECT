 
import logging
from typing import Dict, Any
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from config.settings import DB_CONFIG

class DatabaseConnector:
    """
    Manages database connections and operations
    """
    _instance = None

    def __new__(cls):
        """Singleton pattern implementation"""
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._init_connection()
        return cls._instance

    def _init_connection(self):
        """Initialize database connection"""
        try:
            # Create connection string
            self.connection_string = (
                f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                self.connection_string, 
                pool_size=10,  # Number of connections in the pool
                max_overflow=20,  # Number of connections that can be created beyond pool_size
                pool_timeout=30,  # Timeout for getting a connection from the pool
                pool_recycle=1800  # Recycle connections after 30 minutes
            )
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            logging.info("Database connection initialized successfully")
        except Exception as e:
            logging.error(f"Database connection initialization failed: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logging.error(f"Database connection test failed: {e}")
            return False

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> Any:
        """
        Execute a SQL query
        
        Args:
            query (str): SQL query to execute
            params (Dict[str, Any], optional): Query parameters
        
        Returns:
            Query result
        """
        try:
            with self.engine.connect() as connection:
                if params:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                return result
        except Exception as e:
            logging.error(f"Query execution failed: {e}")
            raise

    def create_tables(self):
        """
        Create essential tables for the ETL pipeline
        """
        create_tables_sql = """
        -- Bronze Layer Tables
        CREATE TABLE IF NOT EXISTS bronze_histo_cotation (
            seance VARCHAR(50),
            groupe VARCHAR(100),
            code VARCHAR(50),
            valeur VARCHAR(100),
            ouverture VARCHAR(50),
            cloture VARCHAR(50),
            plus_bas VARCHAR(50),
            plus_haut VARCHAR(50),
            quantite_negociee VARCHAR(50),
            nb_transaction VARCHAR(50),
            capitaux VARCHAR(50),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS bronze_histo_indice (
            seance VARCHAR(50),
            code_indice VARCHAR(50),
            lib_indice VARCHAR(100),
            indice_jour VARCHAR(50),
            indice_veille VARCHAR(50),
            variation_veille VARCHAR(50),
            indice_plus_haut VARCHAR(50),
            indice_plus_bas VARCHAR(50),
            indice_ouv VARCHAR(50),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Silver Layer Tables
        CREATE TABLE IF NOT EXISTS silver_histo_cotation (
            seance DATE,
            groupe VARCHAR(100),
            code VARCHAR(50),
            valeur VARCHAR(100),
            ouverture NUMERIC(10,2),
            cloture NUMERIC(10,2),
            plus_bas NUMERIC(10,2),
            plus_haut NUMERIC(10,2),
            quantite_negociee NUMERIC(15,2),
            nb_transaction INTEGER,
            capitaux NUMERIC(15,2),
            day_of_week INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            is_month_end BOOLEAN,
            is_month_start BOOLEAN,
            range_day NUMERIC(10,2),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS silver_histo_indice (
            seance DATE,
            code_indice INTEGER,
            lib_indice VARCHAR(100),
            indice_jour NUMERIC(10,2),
            indice_veille NUMERIC(10,2),
            variation_veille NUMERIC(5,2),
            indice_plus_haut NUMERIC(10,2),
            indice_plus_bas NUMERIC(10,2),
            indice_ouv NUMERIC(10,2),
            day_of_week INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            is_month_end BOOLEAN,
            is_month_start BOOLEAN,
            range_day NUMERIC(10,2),
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Error Logging Table
        CREATE TABLE IF NOT EXISTS etl_error_log (
            id SERIAL PRIMARY KEY,
            error_message TEXT,
            error_details TEXT,
            occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            with self.engine.connect() as connection:
                connection.execute(text(create_tables_sql))
            logging.info("Tables created successfully")
        except Exception as e:
            logging.error(f"Error creating tables: {e}")
            raise

# Usage example
def initialize_database():
    """
    Initialize database connection and create tables
    """
    try:
        # Create database connector
        db_connector = DatabaseConnector()
        
        # Test connection
        if db_connector.test_connection():
            logging.info("Database connection successful")
            
            # Create tables
            db_connector.create_tables()
        else:
            logging.error("Database connection failed")
    
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")

# Run initialization when the module is imported
initialize_database()