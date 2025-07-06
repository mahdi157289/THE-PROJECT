import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project Root Directory
BASE_DIR = Path(__file__).parent.parent

# Database Configuration
DB_CONFIG = {
    'username': os.getenv('DB_USERNAME', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Mahdi1574$'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'pfe_database')
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'Medallion ETL Pipeline',
    'master': 'local[*]',  # Use all available cores
    'packages': [
        'org.postgresql:postgresql:42.5.4'
    ],
    'memory': {
        'driver': '4g',
        'executor': '4g'
    },
    'config': {
        'spark.sql.execution.arrow.maxRecordsPerBatch': '1000',
        'spark.sql.execution.arrow.timeout': '1200s',  # Increased timeout
        'spark.driver.maxResultSize': '2g',
        'spark.sql.execution.arrow.pyspark.fallback.enabled': 'true'
    }
}

# Paths Configuration
PATHS = {
    'input_data': BASE_DIR / 'data' / 'input',
    'bronze_data': BASE_DIR / 'data' / 'bronze',
    'silver_data': BASE_DIR / 'data' / 'silver',
    'gold_data': BASE_DIR / 'data' / 'gold',
    'logs': BASE_DIR / 'data' / 'logs',
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO'
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': PATHS['logs'] / 'etl_pipeline.log',
            'formatter': 'standard',
            'level': 'DEBUG'
        }
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}