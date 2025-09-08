 
import logging
import traceback
from typing import Optional
from config.settings import LOGGING_CONFIG
import logging.config

class ETLLogger:
    """
    Custom logger for ETL processes with enhanced logging capabilities
    """
    def __init__(self, name: str = 'ETLLogger'):
        """
        Initialize logger with configuration
        
        Args:
            name (str): Logger name
        """
        # Configure logging
        logging.config.dictConfig(LOGGING_CONFIG)
        
        # Create logger
        self.logger = logging.getLogger(name)
    
    def info(self, message: str):
        """Log informational message"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(
        self, 
        message: str, 
        exception: Optional[Exception] = None
    ):
        """
        Log error with optional exception details
        
        Args:
            message (str): Error message
            exception (Exception, optional): Exception object
        """
        if exception:
            error_details = {
                'message': message,
                'exception_type': type(exception).__name__,
                'exception_details': str(exception),
                'traceback': traceback.format_exc()
            }
            self.logger.error(f"{message}\n{error_details}")
        else:
            self.logger.error(message)
    
    def log_etl_error(
        self, 
        stage: str, 
        error: Exception
    ):
        """
        Log ETL process errors with additional context
        
        Args:
            stage (str): ETL stage where error occurred
            error (Exception): Exception object
        """
        error_message = (
            f"ETL Error in {stage} stage\n"
            f"Error Type: {type(error).__name__}\n"
            f"Error Details: {str(error)}"
        )
        
        # Log error
        self.error(error_message, error)
        
        # Optionally store in database error log
        try:
            from src.utils.db_connection import DatabaseConnector
            
            db_connector = DatabaseConnector()
            db_connector.execute_query(
                """
                INSERT INTO etl_error_log 
                (error_message, error_details) 
                VALUES (:message, :details)
                """,
                {
                    'message': error_message,
                    'details': traceback.format_exc()
                }
            )
        except Exception as log_error:
            self.error("Failed to log error to database", log_error)

# Global logger instance
etl_logger = ETLLogger()