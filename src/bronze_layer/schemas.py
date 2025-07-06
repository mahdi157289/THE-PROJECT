from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, DateType, IntegerType, DoubleType,
    TimestampType
)

def create_bronze_schemas():
    """
    Define schemas for bronze layer tables with appropriate data types
    
    The schema definitions use StringType for numeric fields that might
    contain formatting issues in the raw data. These will be converted
    to appropriate numeric types during preprocessing.
    
    Returns:
        tuple: (cotations_schema, indices_schema)
    """
    # Cotations Schema
    cotations_schema = StructType([
        # Date and identification fields
        StructField("seance", DateType(), True),
        StructField("groupe", StringType(), True),
        StructField("code", StringType(), True),
        StructField("valeur", StringType(), True),
        
        # Numeric fields - kept as StringType for initial loading
        # These will be converted during preprocessing
        StructField("ouverture", StringType(), True),
        StructField("cloture", StringType(), True),
        StructField("plus_bas", StringType(), True),
        StructField("plus_haut", StringType(), True),
        StructField("quantite_negociee", StringType(), True),
        StructField("nb_transaction", StringType(), True),
        StructField("capitaux", StringType(), True),
        
        # Year field added during preprocessing
        StructField("annee", IntegerType(), True),
        
        # Metadata fields added after schema application
        # StructField("ingestion_timestamp", TimestampType(), True),
        # StructField("source_file", StringType(), True),
    ])

    # Indices Schema
    indices_schema = StructType([
        # Date and identification fields
        StructField("seance", DateType(), True),
        StructField("code_indice", StringType(), True),
        StructField("lib_indice", StringType(), True),
        
        # Numeric fields - kept as StringType for initial loading
        # These will be converted during preprocessing
        StructField("indice_jour", StringType(), True),
        StructField("indice_veille", StringType(), True),
        StructField("variation_veille", StringType(), True),
        StructField("indice_plus_haut", StringType(), True),
        StructField("indice_plus_bas", StringType(), True),
        StructField("indice_ouv", StringType(), True),
        
        # Year field added during preprocessing
        StructField("annee", IntegerType(), True),
        
        # Metadata fields added after schema application
        # StructField("ingestion_timestamp", TimestampType(), True),
        # StructField("source_file", StringType(), True),
    ])

    return cotations_schema, indices_schema

def create_silver_schemas():
    """
    Define schemas for silver layer tables with correct data types
    after data cleansing in the bronze layer
    
    Returns:
        tuple: (cotations_schema, indices_schema)
    """
    # Cotations Schema with proper numeric types
    cotations_schema = StructType([
        # Date and identification fields
        StructField("seance", DateType(), False),  # Not nullable in silver
        StructField("groupe", StringType(), True),
        StructField("code", StringType(), False),  # Not nullable in silver
        StructField("valeur", StringType(), True),
        
        # Now using proper numeric types
        StructField("ouverture", DoubleType(), True),
        StructField("cloture", DoubleType(), True),
        StructField("plus_bas", DoubleType(), True),
        StructField("plus_haut", DoubleType(), True),
        StructField("quantite_negociee", DoubleType(), True),
        StructField("nb_transaction", IntegerType(), True),
        StructField("capitaux", DoubleType(), True),
        
        # Year field
        StructField("annee", IntegerType(), False),  # Not nullable in silver
        
        # Metadata fields
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
    ])

    # Indices Schema with proper numeric types
    indices_schema = StructType([
        # Date and identification fields
        StructField("seance", DateType(), False),  # Not nullable in silver
        StructField("code_indice", StringType(), False),  # Not nullable in silver
        StructField("lib_indice", StringType(), True),
        
        # Now using proper numeric types
        StructField("indice_jour", DoubleType(), True),
        StructField("indice_veille", DoubleType(), True),
        StructField("variation_veille", DoubleType(), True),
        StructField("indice_plus_haut", DoubleType(), True),
        StructField("indice_plus_bas", DoubleType(), True),
        StructField("indice_ouv", DoubleType(), True),
        
        # Year field
        StructField("annee", IntegerType(), False),  # Not nullable in silver
        
        # Metadata fields
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False),
    ])

    return cotations_schema, indices_schema