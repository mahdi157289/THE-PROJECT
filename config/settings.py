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


# Paths Configuration
PATHS = {
    'input_data': BASE_DIR / 'data' / 'input',
    'bronze_data': BASE_DIR / 'data' / 'bronze',
    'silver_data': BASE_DIR / 'data' / 'silver',
    'gold_data': BASE_DIR / 'data' / 'gold',
    'logs': BASE_DIR / 'data' / 'logs',
    'warehouse': BASE_DIR / 'spark-warehouse'
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






# Spark Configuration (Updated for Spark 3.4.0)
SPARK_CONFIG = {
    'app_name': 'Medallion ETL Pipeline',
    'master': 'spark://spark-master:7077',  # Use service name for Docker networking
    'packages': [
        'org.postgresql:postgresql:42.5.4'
    ],
    'config': {
        # Resource allocation
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2',
        
        # Networking
        'spark.driver.host': 'host.docker.internal',
        'spark.driver.bindAddress': '0.0.0.0',
        'spark.network.timeout': '600s',
        
        # Ivy configuration
        'spark.jars.ivy': '/opt/bitnami/spark/.ivy2',
        'spark.jars.ivySettings': '/opt/bitnami/spark/conf/ivy.xml',
        'spark.jars.repositories': 'https://repo1.maven.org/maven2/',
        
        # Performance tuning
        'spark.sql.shuffle.partitions': '200',
        'spark.default.parallelism': '200',
        'spark.sql.execution.arrow.enabled': 'true',
        'spark.sql.execution.arrow.maxRecordsPerBatch': '10000',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        
        # Windows-specific
        'spark.hadoop.fs.file.impl': 'org.apache.hadoop.fs.LocalFileSystem',
        'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2'
    }
}