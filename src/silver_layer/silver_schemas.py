from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, DateType, IntegerType, 
    BooleanType, TimestampType
)

def create_silver_schemas():
    """
    Define schemas for silver layer tables with enhanced data types
    and stricter constraints
    """
    # Silver Cotations Schema
    silver_cotations_schema = StructType([
        # Core Identification Fields
        StructField("seance", DateType(), False),  # Not nullable
        StructField("groupe", StringType(), True),
        StructField("code", StringType(), False),  # Not nullable
        StructField("valeur", StringType(), True),
        
        # Numeric Fields with Proper Types
        StructField("ouverture", DoubleType(), True),
        StructField("cloture", DoubleType(), True),
        StructField("plus_bas", DoubleType(), True),
        StructField("plus_haut", DoubleType(), True),
        StructField("quantite_negociee", DoubleType(), True),
        StructField("nb_transaction", IntegerType(), True),
        StructField("capitaux", DoubleType(), True),
        
        # Time-based Derived Fields
        StructField("annee", IntegerType(), False),  # Not nullable
        StructField("mois", IntegerType(), True),
        StructField("jour_semaine", IntegerType(), True),
        StructField("trimestre", IntegerType(), True),
        
        # Boolean Flags
        StructField("est_debut_mois", BooleanType(), True),
        StructField("est_fin_mois", BooleanType(), True),
        
        # Calculated Fields
        StructField("variation_jour", DoubleType(), True),
        StructField("variation_pourcentage", DoubleType(), True),
        
        # Metadata Fields
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False)
    ])

    # Silver Indices Schema
    silver_indices_schema = StructType([
        # Core Identification Fields
        StructField("seance", DateType(), False),  # Not nullable
        StructField("code_indice", StringType(), False),  # Not nullable
        StructField("lib_indice", StringType(), True),
        
        # Numeric Fields with Proper Types
        StructField("indice_jour", DoubleType(), True),
        StructField("indice_veille", DoubleType(), True),
        StructField("variation_veille", DoubleType(), True),
        StructField("indice_plus_haut", DoubleType(), True),
        StructField("indice_plus_bas", DoubleType(), True),
        StructField("indice_ouv", DoubleType(), True),
        
        # Time-based Derived Fields
        StructField("annee", IntegerType(), False),  # Not nullable
        StructField("mois", IntegerType(), True),
        StructField("jour_semaine", IntegerType(), True),
        StructField("trimestre", IntegerType(), True),
        
        # Boolean Flags
        StructField("est_debut_mois", BooleanType(), True),
        StructField("est_fin_mois", BooleanType(), True),
        
        # Calculated Fields
        StructField("variation_indice", DoubleType(), True),
        StructField("variation_pourcentage", DoubleType(), True),
        
        # Metadata Fields
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("source_file", StringType(), False)
    ])

    return silver_cotations_schema, silver_indices_schema