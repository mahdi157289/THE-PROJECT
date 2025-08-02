import re
import logging
import time
import pandas as pd
from datetime import datetime
from typing import Tuple
from .models import RejectedRecord

class DataCleaner:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._setup_standardization_rules()

    def _setup_standardization_rules(self):
        """Initialize all standardization rules"""
        self.name_rules = [
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
        
        self.company_mapping = {
            # Banking Sector (1)
            "AMEN BANK": ("TN0003400058", "1"),
            "ATB": ("TN0003600350", "1"),
            "ATTIJARI BANK": ("TN0001600154", "1"),
            "BH": ("TN0001900604", "1"),
            "BH BANK": ("TN0001900604", "1"),
            "BIAT": ("TN0001800457", "1"),
            "BNA": ("TN0003100609", "1"),
            "BQ HABITAT": ("TN0001900612", "1"),  # Real estate financing bank
            "BT": ("TN0002200053", "1"),
            "STB": ("TN0002600955", "1"),
            "UBCI": ("TN0002400505", "1"),
            "UIB": ("TN0003900107", "1"),
            "WIFACK INT BANK": ("TN0007200017", "1"),
            "BTE": ("TN0001300557", "1"),  # Banque de Tunisie et des Emirats
            "TIJARI": ("TN0001600162", "1"),  # Islamic banking division

            # Insurance Sector (2)
            "ASSU MAGHREBIA VIE": ("TNDKJ8O68X14", "2"),
            "ASSUR MAGHREBIA": ("TN0007830011", "2"),
            "ASSURANCES SALIM": ("TN0006550016", "2"),
            "ASTREE": ("TN0003000452", "2"),
            "BH ASSURANCE": ("TN0006550016", "2"),
            "SALIM": ("TN0006550016", "2"),
            "TUNIS RE": ("TN0007380017", "2"),
            "STAR": ("TN0006060016", "2"),
            "ASS MULTI ITTIHAD": ("TN0007680010", "2"),  # BNA Assurances subsidiary
            "SITS": ("TN0007180011", "2"),  # Reinsurance services
            "LA CARTE": ("TN0001700301", "2"),  # Digital insurance solutions

            # Leasing & Financial Services (3)
            "ATTIJARI LEASING": ("TN0006610018", "3"),
            "BEST LEASE": ("TN0007580012", "3"),
            "BH LEASING": ("TN0006720049", "3"),
            "EL WIFACK LEASING": ("TN0007200017", "3"),
            "GENERAL LEASING": ("TN0006610018", "3"),
            "HANNIBAL LEASE": ("TN0007310139", "3"),
            "MODERN LEASING": ("TN0006720049", "3"),
            "TUNISIE LEASING": ("TN0002100907", "3"),
            "ATTIJARI LS ROMPU": ("TN0006610141", "3"),
            "ATL": ("TN0004700100", "3"),  # Arab Tunisian Lease
            "PLAC. TSIE-SICAF": ("TN0002500650", "3"),  # Investment funds

            # Industrial & Manufacturing (4)
            "ALKIM": ("TN0003800711", "4"),  # Chemical manufacturing
            "CARTHAGE CEMENT": ("TN0007400013", "4"),
            "CIMENTS DE BIZERTE": ("TN0007350010", "4"),
            "ELBENE INDUSTRIE": ("TN0003300902", "4"),  # Food processing
            "SITEX": ("TN0004300307", "4"),  # Textiles
            "SOTIPAPIER": ("TN0007630015", "4"),  # Paper manufacturing
            "SOTRAPIL": ("TN0006660013", "4"),  # Oil pipelines
            "SOTUVER": ("TN0006560015", "4"),  # Glass manufacturing
            "STE TUN. DU SUCRE": ("TN0006170013", "4"),  # Sugar production
            "TUNISIE LAIT": ("TN0003300902", "4"),  # Dairy products
            "OFFICEPLAST": ("TN0007700016", "4"),  # Plastic manufacturing
            "SIAME": ("TN0006590012", "4"),  # Metalworks
            "STEQ": ("TN0006640015", "4"),  # Industrial equipment
            "STIP": ("TN0005030010", "4"),  # Pipe manufacturing
            "TPR": ("TN0007270010", "4"),  # Aluminum production
            "CIL": ("TN0004200853", "4"),  # Chemical products

            # Consumer Goods & Retail (5)
            "CITY CARS": ("TN0007550015", "5"),
            "ENNAKL AUTOMOBILES": ("TN0007410012", "5"),
            "EURO-CYCLES": ("TN0007570013", "5"),
            "MAGASIN GENERAL": ("TN0006440010", "5"),
            "MONOPRIX": ("TN0001000116", "5"),
            "NEW BODY LINE": ("TN0007540016", "5"),
            "SFBT": ("TN0001100254", "5"),  # Beverage company
            "PALM BEACH": ("TN0002900361", "5"),  # Retail/leisure
            "ASSAD": ("TN0007140015", "5"),  # Textile retail
            "SAH": ("TN0007610017", "5"),  # Hygiene products
            "UADH": ("TN0007690019", "5"),  # Automotive retail (Loukil Group)
            "ATELIER MEUBLE INT": ("TN0007740012", "5"),  # Furniture

            # Technology & Telecom (6)
            "AETECH": ("TN0007500010", "6"),
            "CELLCOM": ("TN0007590011", "6"),
            "HEXABYTE": ("TN0007490012", "6"),
            "SERVICOM": ("TN0007340011", "6"),
            "SOTETEL": ("TN0006530018", "6"),
            "SOTEMAIL": ("TN0007600018", "6"),
            "TELNET HOLDING": ("TN0007440019", "6"),
            "AMS": ("TN0001500859", "6"),  # IT services
            "SMART TUNISIE": ("TNQPQXRODTH8", "6"),  # Telecom services
            "ELECTROSTAR": ("TN0006650014", "6"),  # Electronics

            # Construction & Real Estate (7)
            "ARTES": ("TN0007300015", "7"),
            "EL MAZRAA": ("TN0006480015", "7"),  # Agricultural real estate
            "ESSOUKNA": ("TN0007210016", "7"),
            "LAND OR": ("TN0007510019", "7"),
            "MPBS": ("TN0007620016", "7"),  # Building materials
            "SOPAT": ("TN0007290018", "7"),  # Public works
            "SOMOCER": ("TN0006780019", "7"),  # Ceramics/construction
            "ICF": ("TN0003200755", "7"),  # Cement/building materials

            # Transportation & Logistics (8)
            "AIR LIQUIDE TSIE": ("TN0002300358", "8"),  # Industrial gas transport
            "KARTHAGO AIRLINES": ("TN0007160013", "8"),
            "SOTUMAG": ("TN0006580013", "8"),  # Maritime transport
            "SYPHAX AIRLINES": ("TN0007560014", "8"),
            "TUNISAIR": ("TN0001200401", "8"),
            "SIMPAR": ("TN0004000055", "8"),  # Logistics/freight

            # Pharmaceuticals & Healthcare (9)
            "ADWYA": ("TN0007250012", "9"),
            "SANIMED": ("TN0007730013", "9"),
            "UNIMED": ("TN0007720014", "9"),

            # Holding Companies (10)
            "DELICE HOLDING": ("TN0007670011", "10"),
            "GIF": ("TN0007130016", "10"),
            "GIF-FILTER": ("TN0007130016", "10"),
            "ONE TECH HOLDING": ("TN0007530017", "10"),
            "POULINA GP HOLDING": ("TN0005700018", "10"),
            "TAWASOL GP HOLDING": ("TN0007650013", "10"),
            "MAGHREB INTERN PUB": ("TN0007660012", "10"),  # Media/assets holding

            # Energy (11)
            "SIPHAT": ("TN0006670012", "11"),  # Petroleum products

            # Miscellaneous (99) - Only for unverifiable entries
            "Autres Lignes": ("999999", "99")  # Placeholder
        }
    def clean_data(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Main cleaning pipeline with enhanced logging"""
        start_time = time.time()
        total_records = len(df)
        
        self.logger.info(f"🧼 Starting data cleaning for {total_records:,} records from {source_file}")
        self.logger.debug(f"Initial data sample:\n{df.head(2).to_string()}")
        
        # Step 1: Initial filtering
        clean_df, rejected_df = self._filter_invalid_records(df, source_file)
        self.logger.debug(f"After initial filtering: {len(clean_df):,} clean, {len(rejected_df):,} rejected")
        # Log full rejection stats but only save sample
        if not rejected_df.empty:
            self.logger.info(f"Found {len(rejected_df):,} rejected records ({(len(rejected_df)/total_records):.2%})")
            self.logger.debug(f"Rejection sample:\n{rejected_df.head(10).to_string()}")
            
        # Step 2: Standardization
        if not clean_df.empty:
            self.logger.debug("Applying standardization rules")
            clean_df = self._standardize_data(clean_df)
            self.logger.debug(f"After standardization: {len(clean_df):,} records remain")
        
        # Step 3: Report results
        self._report_cleaning_results(
            total_records=total_records,
            clean_records=len(clean_df),
            rejected_records=len(rejected_df),
            start_time=start_time
        )
        
        # Additional diagnostics
        null_capitaux = clean_df['capitaux'].isna().sum()
        if null_capitaux > 0:
            self.logger.warning(f"Found {null_capitaux} records with null capitaux in clean data")
        
        return clean_df, rejected_df

    def _filter_invalid_records(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Filter out invalid records"""
        # Filter records with numerical characters in 'valeur'
        invalid_mask = df['valeur'].str.contains(r'\d', na=False)
        valid_df = df[~invalid_mask].copy()
        invalid_df = df[invalid_mask].copy()
        
        # Prepare rejected records
        rejected_df = invalid_df.copy()
        rejected_df['rejection_reason'] = "Numeric characters in 'valeur'"
        rejected_df['rejection_timestamp'] = datetime.now()
        rejected_df['source_file'] = source_file
        
        return valid_df, rejected_df

    def _standardize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all standardization rules"""
        df = self._standardize_names(df)
        df = self._standardize_codes_and_groups(df)
        return df

    def _standardize_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize company names"""
        for pattern, replacement in self.name_rules:
            df['valeur'] = df['valeur'].str.replace(pattern, replacement, regex=True)
        return df

    def _standardize_codes_and_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize company codes and groups"""
        for company, (std_code, std_group) in self.company_mapping.items():
            mask = df['valeur'] == company
            df.loc[mask, 'code'] = std_code
            df.loc[mask, 'groupe'] = std_group
        return df

    def _report_cleaning_results(self, total_records: int, clean_records: int, 
                               rejected_records: int, start_time: float):
        """Log cleaning results"""
        duration = time.time() - start_time
        
        self.logger.info("\n📊 DATA CLEANING REPORT")
        self.logger.info(f"   Total records processed: {total_records:,}")
        self.logger.info(f"   Clean records: {clean_records:,} ({clean_records/total_records:.1%})")
        self.logger.info(f"   Rejected records: {rejected_records:,} ({rejected_records/total_records:.1%})")
        self.logger.info(f"   Processing time: {duration:.2f} seconds")
        if rejected_records > 0:
            self.logger.info(f"   Clean/Reject ratio: {clean_records/rejected_records:.2f}:1")