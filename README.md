 
# Medallion ETL Project

## Project Overview
This project implements a Medallion architecture for ETL processes using PySpark and PostgreSQL.

## Setup Instructions
1. Clone the repository
2. Create virtual environment: \`python3 -m venv venv\`
3. Activate virtual environment : venv\Scripts\activate
4. Install dependencies: \`pip install -r requirements.txt\`
5. Configure .env file with database credentials
6. Run the main script: \`python -m src.main\`

## Project Structure
- \`src/\`: Main source code
-utlis db_connection :
    Created a singleton DatabaseConnector class
    Implemented database connection using SQLAlchemy
    Added connection pooling for efficiency
    Created methods for:
    Testing connection
    Executing queries
    Creating initial tables
-utils error_logging :
    Implemented a custom ETL logger
    Supports logging to console and file
    Provides methods for different log levels
    Includes database error logging    
- \`config/\`: Configuration files
    Centralized configuration using environment variables
    Defines database connection parameters
    Sets up Spark configuration
    Defines project directory paths
    Configures logging settings
- \`data/\`: Data storage directories
- \`tests/\`: Unit tests

## Dependencies
- PySpark
- PostgreSQL
- SQLAlchemy
- python-dotenv