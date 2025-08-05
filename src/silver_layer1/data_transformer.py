from datetime import datetime
import logging
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field, validator, ValidationError
from typing import List, Dict, Optional, Tuple

# --------------------------
# Pydantic Data Models
# --------------------------
class CotationItem(BaseModel):
    """Validation model for individual cotation records"""
    seance: datetime
    groupe: str
    code: str
    valeur: str
    ouverture: Optional[float] = Field(None, ge=0)
    cloture: Optional[float] = Field(None, ge=0)
    plus_bas: Optional[float] = Field(None, ge=0)
    plus_haut: Optional[float] = Field(None, ge=0)
    quantite_negociee: Optional[float] = Field(None, ge=0)
    nb_transaction: Optional[int] = Field(None, ge=0)
    capitaux: Optional[float] = Field(None, ge=0)

    @validator('*', pre=True)
    def handle_nan(cls, v):
        """Convert numpy/pandas nulls to None"""
        if pd.isna(v) or (isinstance(v, (np.floating, np.integer)) and np.isnan(v)):
            return None
        if isinstance(v, np.generic):
            return v.item()
        return v

    @validator('cloture')
    def validate_price_range(cls, v, values):
        """Business rule: closing price must be between low and high"""
        if v is None:
            return None
        if 'plus_bas' in values and values['plus_bas'] is not None and v < values['plus_bas']:
            raise ValueError("Closing price cannot be lower than daily low")
        if 'plus_haut' in values and values['plus_haut'] is not None and v > values['plus_haut']:
            raise ValueError("Closing price cannot exceed daily high")
        return v

    @validator('nb_transaction', pre=True)
    def handle_nb_transaction_nan(cls, v):
        if v is None or pd.isna(v):
            return None
        return int(v)

class IndiceItem(BaseModel):
    """Validation model for individual indice records"""
    seance: datetime
    code_indice: int
    lib_indice: Optional[str] = None
    indice_jour: Optional[float] = Field(None, ge=0)
    indice_veille: Optional[float] = Field(None, ge=0)
    variation_veille: Optional[float] = None
    indice_plus_haut: Optional[float] = Field(None, ge=0)
    indice_plus_bas: Optional[float] = Field(None, ge=0)
    indice_ouv: Optional[float] = Field(None, ge=0)

    @validator('*', pre=True)
    def handle_nan(cls, v):
        if pd.isna(v) or (isinstance(v, np.generic) and np.isnan(v)):
            return None
        if isinstance(v, np.generic):
            return v.item()
        return v
    @validator('code_indice', pre=True)
    def convert_to_int(cls, v):
        """Convert to integer while handling numpy/pandas types"""
        if isinstance(v, (np.integer, int)):
            return int(v)
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None

# --------------------------
# Data Transformer Class
# --------------------------
class DataTransformer:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def transform_cotations(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Transform cotations data with validation and NULL checks"""
        self.logger.info(f"Starting cotation transformation ({len(df)} records)")
        
        # Validate and separate NULL records
        valid_df, rejected_prime_df = self._validate_dataframe(df, CotationItem)
        
        # Process only valid data
        if not valid_df.empty:
            valid_df = self._convert_numeric_columns(valid_df, is_indices=False)
            valid_df = self._add_date_features(valid_df)
            valid_df = self._calculate_variations(valid_df, 'code', 'cloture', 'price')
            valid_df['ingestion_timestamp'] = datetime.now()
        
        # Add source file to both valid and rejected
        valid_df['source_file'] = source_file
        rejected_prime_df['source_file'] = source_file
        
        self.logger.info(f"Transformed {len(valid_df)} cotations, rejected {len(rejected_prime_df)}")
        return valid_df, rejected_prime_df

    def transform_indices(self, df: pd.DataFrame, source_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Transform indices data with validation and NULL checks"""
        self.logger.info(f"Starting indices transformation ({len(df)} records)")
        
        # Validate and separate NULL records
        valid_df, rejected_prime_df = self._validate_dataframe(df, IndiceItem)
        
        # Process only valid data
        if not valid_df.empty:
            valid_df = self._convert_numeric_columns(valid_df, is_indices=True)
            valid_df = self._add_date_features(valid_df)
            valid_df = self._calculate_index_variations(valid_df)
            valid_df['ingestion_timestamp'] = datetime.now()
        
        # Add source file to both valid and rejected
        valid_df['source_file'] = source_file
        rejected_prime_df['source_file'] = source_file
        
        self.logger.info(f"Transformed {len(valid_df)} indices, rejected {len(rejected_prime_df)}")
        return valid_df, rejected_prime_df

    def _validate_dataframe(self, df: pd.DataFrame, model: BaseModel) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Validate DataFrame and return (valid_df, rejected_df)"""
        df_cleaned = df.dropna(how="any")  # Drops rows with ANY Null/None values
        valid_rows = []
        rejected_rows = []
        
        for idx, row in df_cleaned.iterrows():
            row_data = {
                k: v.item() if isinstance(v, np.generic) else v
                for k, v in row.to_dict().items()
            }
            
            try:
                validated = model(**row_data)
                # Check for None values in non-nullable fields
                if any(v is None for v in validated.dict().values()):
                    rejected_rows.append({
                        **row_data,
                        'rejection_reason': f"NULL_VALUE_IN_{[k for k,v in validated.dict().items() if v is None]}",
                        'rejection_timestamp': datetime.now(),
                        'source_file': 'validation'
                    })
                else:
                    valid_rows.append(validated.dict())
                    
            except ValidationError as e:
                rejected_rows.append({
                    **row_data,
                    'rejection_reason': str(e),
                    'rejection_timestamp': datetime.now(),
                    'source_file': 'validation'
                })
        rejected_df = pd.DataFrame(rejected_rows)
    
        if not rejected_df.empty:
            self.logger.info(f"Found {len(rejected_df):,} validation errors ({(len(rejected_df)/len(df)):.2%})")
            self.logger.debug(f"Validation error sample:\n{rejected_df.head(10).to_string()}")        
        
        return pd.DataFrame(valid_rows), rejected_df

    def _convert_numeric_columns(self, df: pd.DataFrame, is_indices: bool) -> pd.DataFrame:
        """Ensure proper numeric types with robust null handling"""
        if is_indices:
            if 'code_indice' in df.columns:
                df['code_indice'] = pd.to_numeric(df['code_indice'], errors='coerce').astype('Int64')
            
            float_cols = [
                'indice_jour', 'indice_veille', 'variation_veille',
                'indice_plus_haut', 'indice_plus_bas', 'indice_ouv'
            ]
            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            float_cols = [
                'ouverture', 'cloture', 'plus_bas', 'plus_haut',
                'quantite_negociee', 'capitaux'
            ]
            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            if 'nb_transaction' in df.columns:
                df['nb_transaction'] = pd.to_numeric(df['nb_transaction'], errors='coerce').astype('Int32')
        
        return df

    def _add_date_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add temporal features"""
        df['seance'] = pd.to_datetime(df['seance'])
        df['annee'] = df['seance'].dt.year
        df['mois'] = df['seance'].dt.month
        df['jour_semaine'] = df['seance'].dt.dayofweek + 1
        df['trimestre'] = df['seance'].dt.quarter
        df['est_debut_mois'] = df['seance'].dt.day == 1
        df['est_fin_mois'] = df['seance'] == (df['seance'] + pd.offsets.MonthEnd(0))
        return df

    def _calculate_variations(self, df: pd.DataFrame, group_col: str, 
                            value_col: str, prefix: str) -> pd.DataFrame:
        """Calculate price variations"""
        def calculate_group_variations(group):
            group = group.sort_values('seance')
            group['prev_value'] = group[value_col].shift(1)
            group['price_variation_abs'] = round(abs(group[value_col] - group['prev_value']), 2)
            group['variation_pourcentage'] = round(
                (group[value_col] - group['prev_value']) / 
                group['prev_value'].replace(0, 1) * 100, 2)
            return group.drop(columns='prev_value')
        
        return df.groupby(group_col, group_keys=False).apply(calculate_group_variations)

    def _calculate_index_variations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate index-specific variations"""
        def calculate_index_variations(group):
            group = group.sort_values('seance')
            group['prev_indice'] = group['indice_jour'].shift(1)
            group['variation_indice'] = round(abs(group['indice_jour'] - group['prev_indice']), 2)
            return group.drop(columns='prev_indice')
        
        return df.groupby('code_indice', group_keys=False).apply(calculate_index_variations)