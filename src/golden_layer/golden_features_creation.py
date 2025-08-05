"""
Golden features creation for BVMT market data
Complete feature engineering for both cotations and indices
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Set

logger = logging.getLogger('GoldenFeatures')

# Full BVMT sector mapping (stocks → indices)
STOCK_SECTOR_MAP = {
    # Banking (TUNBANQ)
    'ATB': 'TUNBANQ', 'BIAT': 'TUNBANQ', 'BH': 'TUNBANQ', 
    'UBCI': 'TUNBANQ', 'UIB': 'TUNBANQ', 'BT': 'TUNBANQ',
    'BNA': 'TUNBANQ', 'ATTIJARI BANK': 'TUNBANQ',
    
    # Insurance (TUNASS)
    'ASSURANCES SALIM': 'TUNASS', 'ASTREE': 'TUNASS',
    'ASSUR MAGHREBIA': 'TUNASS', 'TUNIS RE': 'TUNASS',
    
    # Financial Services (TUNFIN)
    'TUNISIE LEASING': 'TUNFIN', 'EL WIFACK LEASING': 'TUNFIN',
    'HANNIBAL LEASE': 'TUNFIN',
    
    # Consumer (TUNALIM/TUNCONS)
    'DELICE HOLDING': 'TUNALIM', 'SFBT': 'TUNALIM', 
    'SOTUMAG': 'TUNALIM', 'MONOPRIX': 'TUNCONS',
    
    # Industrials (TUNBATIM)
    'CARTHAGE CEMENT': 'TUNBATIM', 'CIMENTS DE BIZERTE': 'TUNBATIM',
    'SOTUVER': 'TUNBATIM',
    
    # Technology (INDSC)
    'TELNET HOLDING': 'INDSC', 'SOTETEL': 'INDSC', 'SOTEMAIL': 'INDSC',
    
    # Pharmaceuticals (INPMP)
    'UNIMED': 'INPMP', 'SANIMED': 'INPMP',
    
    # Others
    'TUNISAIR': 'INAUE', 'SYPHAX AIRLINES': 'INAUE',
    'POULINA GP HOLDING': 'TUNSAC'
}
class SectorClassifier:
    def __init__(self):
        # Stocks → Sectors mapping
        self.stock_to_sector = {
            'ATB': 'TUNBANQ', 'BIAT': 'TUNBANQ', 'BH': 'TUNBANQ',
            'UBCI': 'TUNBANQ', 'UIB': 'TUNBANQ', 'BT': 'TUNBANQ',
            'BNA': 'TUNBANQ', 'ATTIJARI BANK': 'TUNBANQ',
            'ASSURANCES SALIM': 'TUNASS', 'ASTREE': 'TUNASS',
            'ASSUR MAGHREBIA': 'TUNASS', 'TUNIS RE': 'TUNASS',
            'TUNISIE LEASING': 'TUNFIN', 'EL WIFACK LEASING': 'TUNFIN',
            'HANNIBAL LEASE': 'TUNFIN',
            'DELICE HOLDING': 'TUNALIM', 'SFBT': 'TUNALIM',
            'SOTUMAG': 'TUNALIM', 'MONOPRIX': 'TUNCONS',
            'CARTHAGE CEMENT': 'TUNBATIM', 'CIMENTS DE BIZERTE': 'TUNBATIM',
            'SOTUVER': 'TUNBATIM',
            'TELNET HOLDING': 'INDSC', 'SOTETEL': 'INDSC', 'SOTEMAIL': 'INDSC',
            'UNIMED': 'INPMP', 'SANIMED': 'INPMP',
            'TUNISAIR': 'INAUE', 'SYPHAX AIRLINES': 'INAUE',
            'POULINA GP HOLDING': 'TUNSAC'
        }
        
        # Pure indices that shouldn't be treated as sectors
        self.pure_indices = {'TUNINDEX', 'TUNINDEX20', 'PX1', 'TUN20'}
        
        # All valid sectors derived from stocks
        self.valid_sectors = set(self.stock_to_sector.values())
        self.orphan_sectors = set() 

    def get_sector(self, stock: str) -> str:
        """Get sector for a given stock"""
        return self.stock_to_sector.get(stock)
    
    def is_pure_index(self, index_name: str) -> bool:
        """Check if an index is a pure market index"""
        return index_name in self.pure_indices

    def validate_mappings(self, available_indices: Set[str]) -> bool:
        """Validate all required sector indices exist"""
        self.orphan_sectors = set(available_indices) - self.valid_sectors - self.pure_indices
        missing_indices = self.valid_sectors - set(available_indices)
        if missing_indices:
            logger.warning(f"Missing sector indices: {missing_indices}")
        return len(missing_indices) == 0

    def get_stocks_for_sector(self, sector: str) -> List[str]:
        """Get all stocks belonging to a sector"""
        return [s for s,v in self.stock_to_sector.items() if v == sector]
    
def calculate_rsi(series: pd.Series, window: int = 14) -> pd.Series:
    """Relative Strength Index with BVMT-adjusted thresholds"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_bollinger_bands(series: pd.Series, window: int = 20, num_std: int = 2) -> pd.DataFrame:
    """Bollinger Bands with BVMT volatility scaling"""
    middle = series.rolling(window).mean()
    std = series.rolling(window).std()
    upper = middle + (std * num_std)
    lower = middle - (std * num_std)
    return pd.DataFrame({'upper': upper, 'middle': middle, 'lower': lower})

def calculate_sector_features(stock_prices: pd.Series, sector_indices: Dict[str, pd.Series]) -> pd.DataFrame:
    """Generate sector-relative features with improved date handling"""
    # 1. Remove duplicate dates (keep last occurrence)
    stock_prices = stock_prices[~stock_prices.index.duplicated(keep='last')]
    stock_prices = stock_prices.replace([np.inf, -np.inf], np.nan).dropna()
    
    if len(stock_prices) == 0:
        return pd.DataFrame()
    
    stock_name = stock_prices.name if hasattr(stock_prices, 'name') else 'unknown'
    sector = STOCK_SECTOR_MAP.get(stock_name, None)
    
    if not sector or sector not in sector_indices:
        logger.debug(f"No sector mapping for stock {stock_name}")
        return pd.DataFrame()
    
    # 2. Also deduplicate sector data
    sector_prices = sector_indices[sector]
    sector_prices = sector_prices[~sector_prices.index.duplicated(keep='last')]
    
    # Ensure we have proper datetime indices and align data
    try:
        aligned = pd.DataFrame({
            'stock': stock_prices,
            'sector': sector_prices
        }).dropna()
        
        # Return empty DataFrame if no aligned data
        if aligned.empty:
            logger.debug(f"No aligned data for stock {stock_name} and sector {sector}")
            return pd.DataFrame()
        
        features = pd.DataFrame(index=aligned.index)
        
        # Calculate percentage changes
        stock_returns = aligned['stock'].pct_change()
        sector_returns = aligned['sector'].pct_change()
        
        # Sector features
        features['sector_alpha'] = stock_returns - sector_returns
        
        # Rolling beta (30-day window)
        rolling_cov = stock_returns.rolling(30).cov(sector_returns)
        rolling_var = sector_returns.rolling(30).var()
        features['sector_beta'] = rolling_cov / rolling_var.replace(0, np.nan)
        
        # Rolling correlation (30-day window)
        features['sector_correlation'] = stock_returns.rolling(30).corr(sector_returns)
        
        # Add stock identifier for merging
        features['valeur'] = stock_name
        
        logger.debug(f"Generated sector features for {stock_name}: {len(features)} rows")
        return features
        
    except Exception as e:
        logger.error(f"Error calculating sector features for {stock_name}: {str(e)}")
        return pd.DataFrame()
def _filter_invalid_cotations(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out rows with zeros or nulls in specified columns"""
    cols_to_check = [
        'ouverture', 'cloture', 'plus_bas', 'plus_haut',
        'quantite_negociee', 'nb_transaction', 'capitaux',
    ]
    
    # Create mask for invalid rows
    mask = pd.Series(False, index=df.index)
    for col in cols_to_check:
        if col in df.columns:
            mask |= (df[col] == 0) | df[col].isna()
    
    # Return filtered dataframe
    filtered = df[~mask]
    if len(filtered) < len(df):
        logger.debug(f"Filtered {len(df) - len(filtered)} invalid rows from cotations")
    return filtered

def _filter_invalid_indices(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out rows with zeros or nulls in specified columns"""
    cols_to_check = [
        'variation_veille', 'indice_plus_haut', 'indice_plus_bas',
        'indice_ouv', 'variation_indice',
    ]
    
    # Create mask for invalid rows
    mask = pd.Series(False, index=df.index)
    for col in cols_to_check:
        if col in df.columns:
            mask |= (df[col] == 0) | df[col].isna()
    
    # Return filtered dataframe
    filtered = df[~mask]
    if len(filtered) < len(df):
        logger.debug(f"Filtered {len(df) - len(filtered)} invalid rows from indices")
    return filtered

def enhance_cotations(df: pd.DataFrame, sector_indices: Dict[str, pd.Series]) -> pd.DataFrame:
    """Transform cotations data with all golden features - FIXED VERSION"""
    logger.info("Enhancing cotations with 28+ golden features")
    
    # Validate input columns
    required_columns = {'valeur', 'cloture', 'seance', 'capitaux', 'quantite_negociee', 'nb_transaction'}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing required columns: {missing}")
    
    # Make a copy to avoid modifying original
    enhanced_df = df.copy()
    
    # 1. Technical Features
    logger.info("Adding technical indicators...")
    enhanced_df['rsi_14'] = enhanced_df.groupby('valeur')['cloture'].transform(calculate_rsi)
    
    # Fix Bollinger Bands calculation
    def add_bollinger_bands(group):
        bb = calculate_bollinger_bands(group['cloture'])
        group['bollinger_upper'] = bb['upper']
        group['bollinger_lower'] = bb['lower']
        return group
    
    enhanced_df = enhanced_df.groupby('valeur').apply(add_bollinger_bands).reset_index(drop=True)
    
    # Moving averages
    enhanced_df['ma_7'] = enhanced_df.groupby('valeur')['cloture'].transform(lambda x: x.rolling(7).mean())
    enhanced_df['ma_30'] = enhanced_df.groupby('valeur')['cloture'].transform(lambda x: x.rolling(30).mean())
    
    # Momentum indicators
    enhanced_df['momentum_5'] = enhanced_df.groupby('valeur')['cloture'].transform(lambda x: x.pct_change(5))
    enhanced_df['momentum_30'] = enhanced_df.groupby('valeur')['cloture'].transform(lambda x: x.pct_change(30))
    
    # 2. Liquidity Features
    logger.info("Adding liquidity features...")
    enhanced_df['vwap'] = enhanced_df['capitaux'] / enhanced_df['quantite_negociee'].replace(0, np.nan)
    enhanced_df['liquidity_ratio'] = enhanced_df['quantite_negociee'] / enhanced_df['nb_transaction'].replace(0, np.nan)
    
    # 3. Sector-Relative Features - FIXED
    logger.info("Adding sector-relative features...")
    sector_features_list = []
    
    for stock, group in enhanced_df.groupby('valeur'):
        if stock in STOCK_SECTOR_MAP:
            try:
                # Set the stock name for the series
                stock_series = group.set_index('seance')['cloture']
                stock_series.name = stock
                
                sector_features = calculate_sector_features(stock_series, sector_indices)
                
                if not sector_features.empty:
                    # Reset index to get seance as a column
                    sector_features = sector_features.reset_index()
                    sector_features_list.append(sector_features)
                    
            except Exception as e:
                logger.error(f"Error processing sector features for {stock}: {str(e)}")
                continue
    
    # Merge sector features if any were calculated
    if sector_features_list:
        logger.info(f"Merging sector features from {len(sector_features_list)} stocks")
        all_sector_features = pd.concat(sector_features_list, ignore_index=True)
        
        # Debug: Check the merge columns
        logger.debug(f"Sector features columns: {all_sector_features.columns.tolist()}")
        logger.debug(f"Enhanced DF columns before merge: {enhanced_df.columns.tolist()}")
        
        # Merge on valeur and seance
        enhanced_df = enhanced_df.merge(
            all_sector_features[['valeur', 'seance', 'sector_alpha', 'sector_beta', 'sector_correlation']], 
            on=['valeur', 'seance'], 
            how='left'
        )
        logger.info("Sector features merged successfully")
    else:
        logger.warning("No sector features could be calculated - adding empty columns")
        enhanced_df['sector_alpha'] = np.nan
        enhanced_df['sector_beta'] = np.nan
        enhanced_df['sector_correlation'] = np.nan
    #  filtering at the end    
    enhanced_df = _filter_invalid_cotations(enhanced_df)
    logger.info(f"Final cotations features: {list(enhanced_df.columns)}")
    logger.info(f"Enhanced {len(enhanced_df)} rows with {len(enhanced_df.columns)} total features")
    
    return enhanced_df

def enhance_indices(indices_df: pd.DataFrame) -> pd.DataFrame:
    """Transform indices data with golden features"""
    logger.info("Enhancing indices with sector rotation features")
    
    # Make a copy
    enhanced_indices = indices_df.copy()
    
    # Technical Features
    enhanced_indices['rsi_14'] = enhanced_indices.groupby('lib_indice')['indice_jour'].transform(calculate_rsi)
    enhanced_indices['momentum_20'] = enhanced_indices.groupby('lib_indice')['indice_jour'].transform(
        lambda x: x.pct_change(20))
    
    # Market-Relative Features
    try:
        market_data = enhanced_indices[enhanced_indices['lib_indice'] == 'TUNINDEX'].set_index('seance')
        if not market_data.empty:
            market_returns = market_data['indice_jour'].pct_change()
            
            for sector in enhanced_indices['lib_indice'].unique():
                if sector == 'TUNINDEX':
                    enhanced_indices.loc[enhanced_indices['lib_indice'] == 'TUNINDEX', 'sector_rotation'] = 0.0
                    continue
                    
                sector_mask = enhanced_indices['lib_indice'] == sector
                sector_data = enhanced_indices.loc[sector_mask].set_index('seance')
                
                if not sector_data.empty:
                    sector_returns = sector_data['indice_jour'].pct_change()
                    # Align with market returns and calculate rotation
                    aligned_market = market_returns.reindex(sector_returns.index)
                    rotation = sector_returns - aligned_market
                    
                    # Map back to original DataFrame
                    enhanced_indices.loc[sector_mask, 'sector_rotation'] = rotation.reindex(
                        enhanced_indices.loc[sector_mask, 'seance']).values
        else:
            logger.warning("No TUNINDEX data found for sector rotation calculation")
            enhanced_indices['sector_rotation'] = np.nan
            
    except Exception as e:
        logger.error(f"Error calculating sector rotation: {str(e)}")
        enhanced_indices['sector_rotation'] = np.nan
    # Add filtering at the end
    enhanced_indices = _filter_invalid_indices(enhanced_indices)
    logger.info(f"Final indices features: {list(enhanced_indices.columns)}")
    return enhanced_indices