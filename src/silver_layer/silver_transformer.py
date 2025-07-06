import os
import logging
import time
import re
import sys
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, year, month, dayofweek, quarter, lag, abs, round, current_timestamp,
    lit, input_file_name, regexp_extract, dayofmonth, last_day, when,
    regexp_replace, trim, length, udf, coalesce
)
from pyspark.sql.types import (
    DoubleType, IntegerType, BooleanType, 
    TimestampType, StringType, StructType, StructField
)
from src.utils.spark_session import SparkSessionManager
from src.utils.db_connection import DatabaseConnector
from src.silver_layer.silver_schemas import create_silver_schemas
from config.settings import PATHS, SPARK_CONFIG

class SilverLevelTransformer:
    def __init__(self):
        self.logger = self._setup_logger()
        self.logger.info("🚀 Initializing SilverLevelTransformer")
        
        try:
            self.spark_manager = SparkSessionManager()
            self.spark = self.spark_manager.get_spark_session()
            self.db_connector = DatabaseConnector()
            self._apply_arrow_config()
            self.silver_cotations_schema, self.silver_indices_schema = create_silver_schemas()
            self.logger.info("✅ SilverLevelTransformer initialized successfully")
        except Exception as e:
            self.logger.error("❌ Failed to initialize SilverLevelTransformer", exc_info=True)
            raise

    def _setup_logger(self):
        logger = logging.getLogger('SilverTransformer')
        logger.setLevel(logging.INFO)
        if logger.hasHandlers():
            logger.handlers.clear()
        
        file_handler = logging.FileHandler(
            os.path.join(PATHS['logs'], 'silver_transformer.log'),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def _apply_arrow_config(self):
        arrow_configs = {
            'spark.sql.execution.arrow.maxRecordsPerBatch': '1000',
            'spark.sql.execution.arrow.timeout': '1200s',
            'spark.sql.execution.arrow.pyspark.enabled': 'true'
        }
        for key, value in arrow_configs.items():
            self.spark.conf.set(key, value)

    def _standardize_company_names(self, df: DataFrame) -> DataFrame:
        """Standardize company names according to business rules"""
        self.logger.info("🔄 Standardizing company names")
        
        standardization_rules = [
            (r'(?i)ALKIM\s*(DA)?$', 'ALKIM'),
            (r'(?i)PALM\s*BEACH\s*(\([A-Z]+\))?$', 'PALM BEACH'),
            (r'(?i)TJARI\s*-\s*\(DA\)$', 'TIJARI'),
            (r'(?i)TUNISIE\s*LEASING\s*F$', 'TUNISIE LEASING'),
            (r'(?i)ELBEN\s*INDUSTRIE$', 'ELBENE INDUSTRIE'),
            (r'(?i)BQ\s*DE\s*TSIE\s*-\s*(DA)?$', 'BQ DE TSIE'),
            (r'(?i)BTE\s*\(ADP\)$', 'BTE'),
            (r'(?i)AIR\s*LIQUDE\s*TSIE$', 'AIR LIQUIDE TSIE'),
            (r'(?i)BQ\s*HABITAT\s*\(DA\)$', 'BQ HABITAT'),
            (r'(?i)LA\s*CARTE\s*\(CI\)$', 'LA CARTE'),
            (r'(?i)MONOPRIX\s*(DA)?$', 'MONOPRIX'),
            (r'\s+', ' '),
            (r'^\s+|\s+$', ''),
        ]
        
        standardized_df = df
        for pattern, replacement in standardization_rules:
            standardized_df = standardized_df.withColumn(
                "valeur",
                regexp_replace(col("valeur"), pattern, replacement)
            )

        
        
        return standardized_df

    def _standardize_company_codes_and_groups(self, df: DataFrame) -> DataFrame:
        """Standardize company codes and groups based on reference mapping"""
        self.logger.info("🔄 Standardizing company codes and groups")
        
        company_mapping = {
            "ADWYA": ("TN0007250012", "11"),
            "AETECH": ("TN0007500010", "12"),
            "AIR LIQUIDE TSIE": ("TN0002300358", "12"),
            "ALKIM": ("TN0003800711", "32"),
            "AMEN BANK": ("TN0003400058", "11"),
            "AMS": ("TN0001500859", "99"),
            "ARTES": ("TN0007300015", "11"),
            "ASS MULTI ITTIHAD": ("TN0007680010", "13"),
            "ASSAD": ("TN0007140015", "99"),
            "ASSU MAGHREBIA VIE": ("TNDKJ8O68X14", "11"),
            "ASSUR MAGHREBIA": ("TN0007830011", "12"),
            "ASSURANCES SALIM": ("TN0006550016", "11"),
            "ASTREE": ("TN0003000452", "12"),
            "ATB": ("TN0003600350", "11"),
            "ATELIER MEUBLE INT": ("TN0007740012", "11"),
            "ATL": ("TN0004700100", "11"),
            "ATTIJARI BANK": ("TN0001600154", "11"),
            "ATTIJARI LEASING": ("TN0006610018", "11"),
            "ATTIJARI LS ROMPU": ("TN0006610141", "32"),
            "Autres Lignes": ("999999", "32"),
            "BEST LEASE": ("TN0007580012", "12"),
            "BH": ("TN0001900604", "11"),
            "BH ASSURANCE": ("TN0006550016", "12"),
            "BH BANK": ("TN0001900604", "11"),
            "BH LEASING": ("TN0006720049", "12"),
            "BIAT": ("TN0001800457", "11"),
            "BNA": ("TN0003100609", "11"),
            "BNA ASSURANCES": ("TN0007680010", "13"),
            "BQ DE TSIE": ("TN0002200103", "32"),
            "BQ HABITAT": ("TN0001900612", "32"),
            "BT": ("TN0002200053", "11"),
            "BTE": ("TN0001300557", "12"),
            "CARTHAGE CEMENT": ("TN0007400013", "11"),
            "CELLCOM": ("TN0007590011", "12"),
            "CIL": ("TN0004200853", "12"),
            "CIMENTS DE BIZERTE": ("TN0007350010", "12"),
            "CITY CARS": ("TN0007550015", "11"),
            "DELICE HOLDING": ("TN0007670011", "11"),
            "EL MAZRAA": ("TN0006480015", "12"),
            "EL WIFACK LEASING": ("TN0007200017", "11"),
            "ELBENE INDUSTRIE": ("TN0003300902", "13"),
            "ELECTROSTAR": ("TN0006650014", "11"),
            "ENNAKL AUTOMOBILES": ("TN0007410012", "11"),
            "ESSOUKNA": ("TN0007210016", "11"),
            "EURO-CYCLES": ("TN0007570013", "11"),
            "GENERAL LEASING": ("TN0006610018", "12"),
            "GIF": ("TN0007130016", "11"),
            "GIF-FILTER": ("TN0007130016", "11"),
            "HANNIBAL LEASE": ("TN0007310139", "12"),
            "HEXABYTE": ("TN0007490012", "52"),
            "ICF": ("TN0003200755", "11"),
            "KARTHAGO AIRLINES": ("TN0007160013", "11"),
            "LA CARTE": ("TN0001700301", "12"),
            "LAND OR": ("TN0007510019", "11"),
            "MAGASIN GENERAL": ("TN0006440010", "12"),
            "MAGHREB INTERN PUB": ("TN0007660012", "52"),
            "MIP": ("TN0007660012", "52"),
            "MODERN LEASING": ("TN0006720049", "11"),
            "MONOPRIX": ("TN0001000116", "32"),
            "MPBS": ("TN0007620016", "11"),
            "NEW BODY LINE": ("TN0007540016", "11"),
            "OFFICEPLAST": ("TN0007700016", "51"),
            "ONE TECH HOLDING": ("TN0007530017", "11"),
            "PALM BEACH": ("TN0002900361", "32"),
            "PLAC. TSIE-SICAF": ("TN0002500650", "12"),
            "POULINA GP HOLDING": ("TN0005700018", "11"),
            "SAH": ("TN0007610017", "11"),
            "SALIM": ("TN0006550016", "13"),
            "SANIMED": ("TN0007730013", "52"),
            "SERVICOM": ("TN0007340011", "52"),
            "SFBT": ("TN0001100254", "11"),
            "SIAME": ("TN0006590012", "11"),
            "SIMPAR": ("TN0004000055", "11"),
            "SIPHAT": ("TN0006670012", "12"),
            "SITEX": ("TN0004300307", "13"),
            "SITS": ("TN0007180011", "12"),
            "SMART TUNISIE": ("TNQPQXRODTH8", "11"),
            "SOMOCER": ("TN0006780019", "11"),
            "SOPAT": ("TN0007290018", "11"),
            "SOTEMAIL": ("TN0007600018", "55"),
            "SOTETEL": ("TN0006530018", "11"),
            "SOTIPAPIER": ("TN0007630015", "11"),
            "SOTRAPIL": ("TN0006660013", "11"),
            "SOTUMAG": ("TN0006580013", "12"),
            "SOTUVER": ("TN0006560015", "11"),
            "SPDIT - SICAF": ("TN0001400704", "12"),
            "STAR": ("TN0006060016", "12"),
            "STB": ("TN0002600955", "11"),
            "STE TUN. DU SUCRE": ("TN0006170013", "13"),
            "STEQ": ("TN0006640015", "12"),
            "STIP": ("TN0005030010", "12"),
            "SYPHAX AIRLINES": ("TN0007560014", "51"),
            "TAWASOL GP HOLDING": ("TN0007650013", "99"),
            "TELNET HOLDING": ("TN0007440019", "11"),
            "TIJARI": ("TN0001600162", "32"),
            "TPR": ("TN0007270010", "11"),
            "TUNINVEST-SICAR": ("TN0004100202", "12"),
            "TUNIS RE": ("TN0007380017", "12"),
            "TUNISAIR": ("TN0001200401", "11"),
            "TUNISIE LAIT": ("TN0003300902", "12"),
            "TUNISIE LEASING": ("TN0002100907", "11"),
            "UADH": ("TN0007690019", "11"),
            "UBCI": ("TN0002400505", "12"),
            "UIB": ("TN0003900107", "11"),
            "UNIMED": ("TN0007720014", "11"),
            "WIFACK INT BANK": ("TN0007200017", "11")
        }
        
        broadcast_mapping = self.spark.sparkContext.broadcast(company_mapping)
        
        def get_standardized_values(valeur):
            mapping = broadcast_mapping.value
            return mapping.get(valeur, (None, None))
        
        standardize_udf = udf(get_standardized_values, StructType([
            StructField("standard_code", StringType()),
            StructField("standard_group", StringType())
        ]))
        
        standardized_df = df.withColumn(
            "standard_values", 
            standardize_udf(col("valeur"))
        )

        standardized_df = standardized_df.withColumn(
            "code",
            when(col("standard_values.standard_code").isNotNull(),
                 col("standard_values.standard_code"))
            .otherwise(col("code"))
        ).withColumn(
            "groupe",
            when(col("standard_values.standard_group").isNotNull(),
                 col("standard_values.standard_group"))
            .otherwise(col("groupe"))
        ).drop("standard_values")
        
        
        
        return standardized_df

    def _filter_and_clean_cotations(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Professional-grade data cleaning with multi-pattern matching"""
        start_time = time.time()
        total_records = df.count()
        self.logger.info(f"🔍 Starting data processing for {total_records:,} records")
        
        # STEP 1: Filter records with numerical characters in 'valeur'
        self.logger.info("🔄 Filtering: Removing records with numeric 'valeur'")
        invalid_valeur = col("valeur").rlike("[0-9]")
        valid_df = df.filter(~invalid_valeur)
        invalid_df = df.filter(invalid_valeur)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        self.logger.info(f"   → Valid records after filtering: {valid_count:,}")
        self.logger.info(f"   → Invalid records (numeric 'valeur'): {invalid_count:,}")
        
        # STEP 2: Professional multi-pattern cleaning system
        self.logger.info("🧹 Professional Cleaning: Multi-pattern malformed data processing")
        
        patterns = [
            r'^(\d{6})\s+([^\d]+?)\s+([\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)$',
            r'^(\d{4,8})\s+([^\d]+?)\s+([\d,]+(?:\.\d+)?)\s+([\d,]+(?:\.\d+)?)$',
            r'^(\d{4,8})\s+([^\d]+?)\s+([\d,]+)\s+([\d,]+)$',
            r'^(\d{4,8})\s+([^\d]+?)\s+\s*([\d,]+)\s+\s*([\d,]+)\s*$',
            r'^(\d{4,8})\s+([^\d]+?)\s+([\d.,]+)\s+([\d.,]+)$',
            r'^(\d{4,8})([^\d]+?)([\d.,]+)([\d.,]+)$',
            r'^(\d{4,8})\s+([^\d]+?)\s+([\d]*\.[\d]+|[\d]+\.?)\s+([\d]*\.[\d]+|[\d]+\.?)$',
            r'^(\d{4,8})\s*([^\d]+?)\s*([\d.,]+)\s*([\d.,]+)$',
            r'^(\d{4,8})\s*([^\d]+?)\s*([\d]+[.,]?\d*)\s*([\d]+[.,]?\d*)$'
        ]
        
        def extract_components(code):
            if not code or str(code).strip().lower() in ['nan', 'n/a', 'null', '']:
                return (0, None, None, None, None)
            
            for i, pattern in enumerate(patterns):
                try:
                    match = re.match(pattern, str(code).strip(), re.IGNORECASE)
                    if match:
                        return (i+1, 
                                match.group(1).strip() if match.group(1) else None,
                                match.group(2).strip() if match.group(2) else None,
                                match.group(3).replace(',', '.') if match.group(3) else None,
                                match.group(4).replace(',', '.') if match.group(4) else None)
                except:
                    continue
            return (0, None, None, None, None)
        
        extract_udf = udf(extract_components, StructType([
            StructField("pattern_id", IntegerType()),
            StructField("code_val", StringType()),
            StructField("name_val", StringType()),
            StructField("open_val", StringType()),
            StructField("close_val", StringType())
        ]))
        
        malformed_condition = (
            (col("ouverture").isNull()) | 
            (col("cloture").isNull()) | 
            ((col("code").rlike(r"\d{4,8}[^\d]+\d")) & 
             (~col("code").rlike(r"^\d{4,8}$")))
        )
        
        clean_df = valid_df.filter(~malformed_condition)
        malformed_df = valid_df.filter(malformed_condition)
        malformed_count = malformed_df.count()
        
        if malformed_count > 0:
            self.logger.info(f"   → Found {malformed_count:,} malformed records")
            
            cleaned_malformed = malformed_df.withColumn(
                "extracted", extract_udf(col("code"))
            ).cache()
            
            pattern_stats = cleaned_malformed.groupBy("extracted.pattern_id").count()
            pattern_stats_list = pattern_stats.collect()
            
            self.logger.info("   → Pattern Effectiveness Report:")
            for stat in pattern_stats_list:
                pid = stat["pattern_id"] or 0
                self.logger.info(f"      Pattern {pid}: {stat['count']} records")
            
            unmatched_df = cleaned_malformed.filter(col("extracted.pattern_id") == 0)
            unmatched_count = unmatched_df.count()
            
            
            cleaned_malformed = cleaned_malformed.select(
                col("seance"),
                col("groupe"),
                coalesce(col("extracted.code_val"), col("code")).alias("code"),
                coalesce(
                    col("extracted.name_val"), 
                    when(
                        (~col("valeur").rlike("[0-9]")) & 
                        (trim(col("valeur")) != "") & 
                        (~trim(col("valeur")).isin("NaN", "N/A", "null")),
                        col("valeur")
                    )
                ).alias("valeur"),
                when(
                    col("extracted.open_val").isNotNull(),
                    col("extracted.open_val").cast(DoubleType())
                ).otherwise(col("ouverture")).alias("ouverture"),
                when(
                    col("extracted.close_val").isNotNull(),
                    col("extracted.close_val").cast(DoubleType())
                ).otherwise(col("cloture")).alias("cloture"),
                col("plus_bas"),
                col("plus_haut"),
                col("quantite_negociee"),
                col("nb_transaction"),
                col("capitaux"),
                col("annee"),
                col("ingestion_timestamp"),
                col("source_file")
            )
            
            
            
            valeur_stats = cleaned_malformed.groupBy("valeur").count().orderBy(col("count").desc()).limit(10)
            self.logger.info("   → Top 10 Valeur Values:")
            for row in valeur_stats.collect():
                self.logger.info(f"      '{row.valeur}': {row['count']} records")
            
            clean_df = clean_df.union(cleaned_malformed)
            self.logger.info(f"   → Professionally cleaned {malformed_count:,} records")
        else:
            self.logger.info("   → No malformed records detected")
        
        # Standardize company names
        clean_df = self._standardize_company_names(clean_df)
        
        # Standardize company codes and groups
        clean_df = self._standardize_company_codes_and_groups(clean_df)
        
        # Prepare rejected records
        rejected_df = invalid_df.withColumn(
            "rejection_reason", lit("Numeric characters in 'valeur'")
        ).withColumn(
            "rejection_timestamp", current_timestamp()
        )
        
        # Final analytics report
        final_valid = clean_df.count()
        final_rejected = rejected_df.count()
        duration = time.time() - start_time
        
        self.logger.info("\n📊 DATA QUALITY REPORT")
        self.logger.info(f"   Total records processed: {total_records:,}")
        self.logger.info(f"   Clean records: {final_valid:,} ({final_valid/total_records:.1%})")
        self.logger.info(f"   Rejected records: {final_rejected:,} ({final_rejected/total_records:.1%})")
        self.logger.info(f"   Processing time: {duration:.2f} seconds")
        if final_rejected > 0:
            self.logger.info(f"   Clean/Reject ratio: {final_valid/final_rejected:.2f}:1")
        
        return clean_df, rejected_df

    def _transform_cotations(self, df: DataFrame) -> DataFrame:
        """Transform cleaned cotations data with proper window calculations"""
        self.logger.info("🔄 Transforming cotations data")
        window_spec = Window.partitionBy("code").orderBy("seance")
        
        transformed_df = df.select(
            col("seance"),
            col("groupe"),
            col("code"),
            col("valeur"),
            col("ouverture").cast(DoubleType()),
            col("cloture").cast(DoubleType()),
            col("plus_bas").cast(DoubleType()),
            col("plus_haut").cast(DoubleType()),
            col("quantite_negociee").cast(DoubleType()),
            col("nb_transaction").cast(IntegerType()),
            col("capitaux").cast(DoubleType()),
            
            year(col("seance")).alias("annee"),
            month(col("seance")).alias("mois"),
            dayofweek(col("seance")).alias("jour_semaine"),
            quarter(col("seance")).alias("trimestre"),
            (dayofmonth(col("seance")) == 1).alias("est_debut_mois"),
            (col("seance") == last_day(col("seance"))).alias("est_fin_mois"),
            
            round(
                abs(col("cloture") - lag(col("cloture"), 1, 0).over(window_spec)) / 
                coalesce(lag(col("cloture"), 1, 1).over(window_spec), lit(1)) * 100, 
                2
            ).alias("variation_jour"),
            round(
                (col("cloture") - lag(col("cloture"), 1, 0).over(window_spec)) / 
                coalesce(lag(col("cloture"), 1, 1).over(window_spec), lit(1)) * 100, 
                2
            ).alias("variation_pourcentage"),
            
            current_timestamp().alias("ingestion_timestamp"),
            regexp_extract(input_file_name(), "([^/]+)$", 0).alias("source_file")
        )
        
        self.logger.info(f"✅ Transformed {transformed_df.count():,} cotations records")
        return transformed_df

    def _transform_indices(self, df: DataFrame) -> DataFrame:
        """Transform indices data with proper window calculations"""
        self.logger.info("🔄 Transforming indices data")
        window_spec = Window.partitionBy("code_indice").orderBy("seance")
        
        transformed_df = df.select(
            col("seance"),
            col("code_indice"),
            col("lib_indice"),
            col("indice_jour").cast(DoubleType()),
            col("indice_veille").cast(DoubleType()),
            col("variation_veille").cast(DoubleType()),
            col("indice_plus_haut").cast(DoubleType()),
            col("indice_plus_bas").cast(DoubleType()),
            col("indice_ouv").cast(DoubleType()),
            
            year(col("seance")).alias("annee"),
            month(col("seance")).alias("mois"),
            dayofweek(col("seance")).alias("jour_semaine"),
            quarter(col("seance")).alias("trimestre"),
            (dayofmonth(col("seance")) == 1).alias("est_debut_mois"),
            (col("seance") == last_day(col("seance"))).alias("est_fin_mois"),
            
            round(
                abs(col("indice_jour") - lag(col("indice_jour"), 1, 0).over(window_spec)) / 
                coalesce(lag(col("indice_jour"), 1, 1).over(window_spec), lit(1)) * 100, 
                2
            ).alias("variation_indice"),
            
            current_timestamp().alias("ingestion_timestamp"),
            regexp_extract(input_file_name(), "([^/]+)$", 0).alias("source_file")
        )
        
        self.logger.info(f"✅ Transformed {transformed_df.count():,} indices records")
        return transformed_df

    def _save_with_retry(self, df: DataFrame, path: str, format: str):
        """Save data with retry logic and proper partitioning"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"💾 Saving data (attempt {attempt+1}/{max_retries}) to {path}")
                (df.repartition(100)
                 .write
                 .mode("overwrite")
                 .option("maxRecordsPerFile", 100000)
                 .format(format)
                 .save(path))
                self.logger.info(f"✅ Successfully saved {df.count():,} rows")
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"⚠️ Attempt failed: {str(e)}")
                time.sleep(5 * (attempt + 1))

    def _save_to_postgres(self, df: DataFrame, table_name: str):
        """Save data to PostgreSQL with proper driver configuration"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.logger.info(f"💾 Saving to PostgreSQL (attempt {attempt+1}/{max_retries}): {table_name}")
                (df.repartition(100)
                 .write
                 .format("jdbc")
                 .mode("overwrite")
                 .option("url", f"jdbc:postgresql://{self.db_connector.db_config['host']}:"
                               f"{self.db_connector.db_config['port']}/"
                               f"{self.db_connector.db_config['database']}")
                 .option("dbtable", table_name)
                 .option("user", self.db_connector.db_config['username'])
                 .option("password", self.db_connector.db_config['password'])
                 .option("driver", "org.postgresql.Driver")
                 .option("isolationLevel", "READ_COMMITTED")
                 .option("batchsize", 10000)
                 .save())
                self.logger.info(f"✅ Successfully saved to PostgreSQL: {table_name}")
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"⚠️ PostgreSQL save failed: {str(e)}")
                time.sleep(5 * (attempt + 1))

    def transform_bronze_to_silver(self):
        """Main transformation pipeline with comprehensive error handling"""
        try:
            self.logger.info("🏁 Starting Bronze to Silver transformation")
            
            # Read source data
            bronze_cotations_path = os.path.join(PATHS['bronze_data'], 'bronze_cotations.parquet')
            bronze_indices_path = os.path.join(PATHS['bronze_data'], 'bronze_indices.parquet')
            
            df_cotations = self.spark.read.parquet(bronze_cotations_path)
            df_indices = self.spark.read.parquet(bronze_indices_path)
            self.logger.info(f"📖 Loaded {df_cotations.count():,} cotations and {df_indices.count():,} indices")
            
            # Filter and clean cotations
            clean_cotations, rejected = self._filter_and_clean_cotations(df_cotations)
            
            # Handle rejected records
            if rejected.count() > 0:
                self.logger.info(f"🗑️ Saving {rejected.count():,} rejected records to database")
                self._save_to_postgres(rejected, 'rejected_cotations')
            
            # Transform datasets
            silver_cotations = self._transform_cotations(clean_cotations)
            silver_indices = self._transform_indices(df_indices)
            
            # Save silver data
            silver_cotations_path = os.path.join(PATHS['silver_data'], 'silver_cotations.parquet')
            silver_indices_path = os.path.join(PATHS['silver_data'], 'silver_indices.parquet')
            
            self._save_with_retry(silver_cotations, silver_cotations_path, "parquet")
            self._save_with_retry(silver_indices, silver_indices_path, "parquet")
            self.logger.info(f"💾 Saved silver data: {silver_cotations.count():,} cotations, {silver_indices.count():,} indices")
            
            # Load to PostgreSQL
            self._save_to_postgres(silver_cotations, 'silver_cotations')
            self._save_to_postgres(silver_indices, 'silver_indices')
            
            self.logger.info("🏆 Bronze to Silver transformation completed successfully")
            return silver_cotations, silver_indices
            
        except Exception as e:
            self.logger.error("❌ Transformation failed", exc_info=True)
            raise

def main():
    transformer = SilverLevelTransformer()
    try:
        transformer.transform_bronze_to_silver()
        return 0
    except Exception as e:
        transformer.logger.error("❌ Pipeline execution failed", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())