from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, DateType, IntegerType
)

def create_bronze_schemas():
    """
    Define schemas for bronze layer tables
    """
    # Cotations Schema
    cotations_schema = StructType([
        StructField("seance", DateType(), True),
        StructField("groupe", StringType(), True),
        StructField("code", StringType(), True),
        StructField("valeur", StringType(), True),
        StructField("ouverture", StringType(), True),
        StructField("cloture", StringType(), True),
        StructField("plus_bas", StringType(), True),
        StructField("plus_haut", StringType(), True),
        StructField("quantite_negociee", StringType(), True),
        StructField("nb_transaction", StringType(), True),
        StructField("capitaux", StringType(), True),
        
    ])

    # Indices Schema
    indices_schema = StructType([
        StructField("seance", DateType(), True),
        StructField("code_indice", StringType(), True),
        StructField("lib_indice", StringType(), True),
        StructField("indice_jour", StringType(), True),
        StructField("indice_veille", StringType(), True),
        StructField("variation_veille", StringType(), True),
        StructField("indice_plus_haut", StringType(), True),
        StructField("indice_plus_bas", StringType(), True),
        StructField("indice_ouv", StringType(), True),
        
    ])

    return cotations_schema, indices_schema