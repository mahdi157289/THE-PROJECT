"""
Statistical Validation Suite for Diamond Layer
Complete market data validation with sector-aware testing and data preparation
"""

import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import anderson, levene
from statsmodels.tsa.stattools import adfuller, grangercausalitytests, coint
import logging
from typing import Dict, Any, List, Set
from functools import lru_cache

# Import sector mappings from Golden Layer
from src.golden_layer.golden_features_creation import STOCK_SECTOR_MAP, SectorClassifier

logger = logging.getLogger('DiamondTests')

class DiamondValidator:
    def __init__(self):
        self.classifier = SectorClassifier()
        self.sector_map = STOCK_SECTOR_MAP
        self._configure_logger()
        self.min_sector_obs = 10  # Minimum observations per sector
        self.min_coint_obs = 30   # Minimum observations for cointegration

    def _configure_logger(self):
        """Set up standardized validation logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('diamond_validation.log'),
                logging.StreamHandler()
            ]
        )

    def _prepare_data(self, df: pd.DataFrame, is_cotations: bool = True) -> pd.DataFrame:
        df = df.copy()
        
        # 1. Sector Mapping Enhancement
        if is_cotations and 'valeur' in df.columns:
            # Map known sectors
            df['sector'] = df['valeur'].map(self.sector_map)
            # Then try to extract sector from groupe/code if available
            if 'groupe' in df.columns:
              df['sector'] = df['sector'].fillna(df['groupe'].str.extract(r'([A-Z]{6})')[0])
            # Handle unknown sectors - don't filter them out
            unknown_mask = df['sector'].isna()
            if unknown_mask.any():
                unknown_count = unknown_mask.sum()
                unknown_percent = (unknown_count / len(df)) * 100
                logger.warning(
                    f"{unknown_count:,} cotations ({unknown_percent:.1f}%) without sector mapping - "
                    "assigning 'UNKNOWN' sector"
                )
                df.loc[unknown_mask, 'sector'] = 'UNKNOWN'
            
            # Verify we have sectors to analyze
            if df['sector'].nunique() == 1 and df['sector'].iloc[0] == 'UNKNOWN':
                logger.error("No valid sector mappings available - all assigned to 'UNKNOWN'")
        
        # 2. Data Quality Checks
        # Ensure required columns exist based on data type
        required_cols = {
            'cotations': {'valeur', 'cloture', 'quantite_negociee', 'sector', 'seance'},
            'indices': {'lib_indice', 'indice_jour', 'seance'}
        }[('cotations' if is_cotations else 'indices')]
        
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # 3. Numeric Validation
        numeric_cols = ['cloture', 'quantite_negociee'] if is_cotations else ['indice_jour']
        for col in numeric_cols:
            if col in df.columns:
                # Replace zeros and negatives with NaN (except for indices which can be zero)
                if is_cotations or col != 'indice_jour':
                    invalid_mask = (df[col] <= 0) | df[col].isna()
                    if invalid_mask.any():
                        logger.debug(f"Filtered {invalid_mask.sum()} invalid {col} values")
                        df.loc[invalid_mask, col] = np.nan
        
        # 4. Date Handling
        if 'seance' in df.columns:
            df['seance'] = pd.to_datetime(df['seance'])
            df = df.sort_values('seance')
            
            # Enhanced gap detection
            date_diff = df['seance'].diff().dt.days
            gaps = date_diff[date_diff > 1]  # Detect any gaps >1 day
            
            if not gaps.empty:
                gap_stats = {
                    'count': len(gaps),
                    'max_gap': gaps.max(),
                    'avg_gap': gaps.mean(),
                    'recent_gap': gaps.iloc[-1] if len(gaps) > 0 else 0
                }
                logger.warning(
                    f"Found {gap_stats['count']} date gaps in data. "
                    f"Max: {gap_stats['max_gap']} days, Avg: {gap_stats['avg_gap']:.1f} days"
                )
                
                # Store gap metrics for later analysis
                df.attrs['date_gaps'] = gap_stats
        
        return df

    # Core Statistical Tests ==============================================
    
    @staticmethod
    def normality_test(series: pd.Series) -> Dict[str, Any]:
        """Anderson-Darling normality test"""
        try:
            result = anderson(series.dropna())
            return {
                'test': 'Anderson-Darling',
                'statistic': result.statistic,
                'critical_values': result.critical_values,
                'is_normal': result.statistic < result.critical_values[2]  # 5% threshold
            }
        except Exception as e:
            logger.error(f"Normality test failed: {str(e)}")
            return {'error': str(e)}

    @staticmethod
    def stationarity_test(series: pd.Series, regression: str = 'c') -> Dict[str, Any]:
        """Augmented Dickey-Fuller stationarity test"""
        try:
            result = adfuller(series.dropna(), regression=regression)
            return {
                'test': 'ADF',
                'statistic': result[0],
                'p_value': result[1],
                'is_stationary': result[1] < 0.05
            }
        except Exception as e:
            logger.error(f"Stationarity test failed: {str(e)}")
            return {'error': str(e)}

    @staticmethod
    def variance_test(groups: List[pd.Series]) -> Dict[str, Any]:
        """Levene's test for equal variances"""
        try:
            stat, p_value = levene(*[g.dropna() for g in groups])
            return {
                'test': 'Levene',
                'statistic': stat,
                'p_value': p_value,
                'equal_variance': p_value > 0.05
            }
        except Exception as e:
            logger.error(f"Variance test failed: {str(e)}")
            return {'error': str(e)}

    def cointegration_test(self, series1: pd.Series, series2: pd.Series) -> Dict[str, Any]:
        """Robust cointegration test with alignment"""
        try:
            aligned = pd.DataFrame({'s1': series1, 's2': series2}).dropna()
            if len(aligned) < self.min_coint_obs:
                return {'error': f"Need {self.min_coint_obs} observations, got {len(aligned)}"}
                
            score, p_value, _ = coint(aligned['s1'], aligned['s2'])
            return {
                'statistic': score,
                'p_value': p_value,
                'is_cointegrated': p_value < 0.05,
                'n_observations': len(aligned)
            }
        except Exception as e:
            return {'error': str(e)}

    @staticmethod
    def granger_test(cause: pd.Series, effect: pd.Series, maxlag: int = 3) -> Dict[str, Any]:
        """Granger causality test with warning suppression"""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            try:
                df = pd.DataFrame({'cause': cause, 'effect': effect}).dropna()
                results = grangercausalitytests(df[['effect', 'cause']], maxlag=maxlag, verbose=False)
                return {
                    f'lag_{lag}': {
                        'p_value': result[0]['ssr_ftest'][1],
                        'causal': result[0]['ssr_ftest'][1] < 0.05
                    }
                    for lag, result in results.items()
                }
            except Exception as e:
                return {'error': str(e)}

    # Sector-Specific Validations =========================================
    
    def validate_sector_relationships(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Comprehensive sector-level validation"""
        results = {}
        valid_sectors = set(self.sector_map.values())
        
        # 1. Sector Volatility Comparison
        sector_returns = {
            sector: df[df['sector'] == sector]['cloture'].pct_change().dropna()
            for sector in valid_sectors
            if len(df[df['sector'] == sector]) >= self.min_sector_obs
        }
        
        if len(sector_returns) >= 2:  # Need at least 2 sectors
            results['sector_volatility'] = self.variance_test(list(sector_returns.values()))
        else:
            results['sector_volatility'] = {'warning': 'Insufficient sectors for comparison'}
        
        # 2. Sector-Market Relationships
        if 'lib_indice' in df.columns:
            market = df[df['lib_indice'] == 'TUNINDEX']
            if not market.empty:
                market_series = market.set_index('seance')['indice_jour']
                for sector in valid_sectors:
                    sector_data = df[df['sector'] == sector]
                    if len(sector_data) >= self.min_sector_obs:
                        results[f"{sector}_market_cointegration"] = self.cointegration_test(
                            market_series,
                            sector_data.set_index('seance')['cloture']
                        )
        
        return results

    # Technical Indicator Validations =====================================
    
    def validate_technical_indicators(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Sanity checks for engineered features"""
        results = {}
        
        # RSI Validation (0-100 range)
        if 'rsi_14' in df.columns:
            invalid_rsi = df[(df['rsi_14'] < 0) | (df['rsi_14'] > 100)]
            results['rsi_validity'] = {
                'invalid_count': len(invalid_rsi),
                'pct_invalid': len(invalid_rsi)/len(df),
                'is_valid': len(invalid_rsi)/len(df) < 0.01
            }
        
        # Bollinger Bands Consistency
        if all(col in df.columns for col in ['cloture', 'bollinger_upper']):
            bb_breaks = df[df['cloture'] > df['bollinger_upper']].shape[0]
            results['bollinger_breaks'] = {
                'upper_breaks': bb_breaks,
                'break_ratio': bb_breaks/len(df)
            }
        
        return results

    # Main Validation Entry Points ========================================
    
    def validate_cotations(self, raw_df: pd.DataFrame) -> Dict[str, Any]:
        """Full validation suite for cotations data"""
        try:
            df = self._prepare_data(raw_df, is_cotations=True)
            
            results = {
                'price_distribution': self.normality_test(df['cloture']),
                'volume_distribution': self.normality_test(df['quantite_negociee']),
                'technical_indicators': self.validate_technical_indicators(df),
                'sector_analysis': self.validate_sector_relationships(df)
            }
            
            # Sample-based stationarity checks
            sample_stocks = df['valeur'].drop_duplicates().sample(min(5, len(df))) if 'valeur' in df.columns else []
            results['stationarity'] = {
                stock: self.stationarity_test(df[df['valeur'] == stock]['cloture'])
                for stock in sample_stocks
            }
            
            return results
        except Exception as e:
            logger.error(f"Cotations validation failed: {str(e)}")
            return {'validation_error': str(e)}

    def validate_indices(self, raw_df: pd.DataFrame) -> Dict[str, Any]:
        """Comprehensive indices validation"""
        try:
            df = self._prepare_data(raw_df, is_cotations=False)
            results = {}
            
            if 'lib_indice' in df.columns:
                market = df[df['lib_indice'] == 'TUNINDEX']
                
                if not market.empty:
                    market_series = market.set_index('seance')['indice_jour']
                    
                    # 1. Market Index Validation
                    results['market_stationarity'] = self.stationarity_test(market_series)
                    
                    # 2. Sector Cointegration
                    for sector in df['lib_indice'].unique():
                        if sector != 'TUNINDEX':
                            sector_data = df[df['lib_indice'] == sector]
                            if len(sector_data) >= self.min_sector_obs:
                                results[f"{sector}_cointegration"] = self.cointegration_test(
                                    market_series,
                                    sector_data.set_index('seance')['indice_jour']
                                )
                    
                    # 3. Financial Sector Causality
                    financial_sectors = ['TUNBANQ', 'TUNASS', 'TUNFIN']
                    valid_pairs = [
                        (s1, s2) 
                        for i, s1 in enumerate(financial_sectors) 
                        for s2 in financial_sectors[i+1:] 
                        if len(df[df['lib_indice'] == s1]) >= self.min_sector_obs and
                           len(df[df['lib_indice'] == s2]) >= self.min_sector_obs
                    ]
                    
                    for s1, s2 in valid_pairs:
                        results[f"{s1}_to_{s2}_granger"] = self.granger_test(
                            df[df['lib_indice'] == s1].set_index('seance')['indice_jour'],
                            df[df['lib_indice'] == s2].set_index('seance')['indice_jour']
                        )
            
            return results
        except Exception as e:
            logger.error(f"Indices validation failed: {str(e)}")
            return {'validation_error': str(e)}

    # Reporting Utilities ================================================
    
    @staticmethod
    def generate_report(results: Dict[str, Any]) -> str:
        """Enhanced terminal-friendly report"""
        report = [
            "\n=== VALIDATION RESULTS ===",
            f"{'Test':<40} {'Status':<10} {'p-value':<10} {'Details'}",
            "-" * 80
        ]

        for category, tests in results.items():
            if category == "advanced_validation":
                continue  # Defer this section for now
            report.append(f"\n◆ {category.upper()}")
            for test_name, result in tests.items():
                if isinstance(result, dict):
                    if 'error' in result:
                        status = "FAIL"
                        details = result['error']
                    else:
                        status = "PASS" if result.get('p_value', 1) < 0.05 else "WARNING"
                        details_parts = []
                        if 'p_value' in result:
                            details_parts.append(f"p={result['p_value']:.4f}")
                        if 'statistic' in result:
                            details_parts.append(f"stat={result['statistic']:.2f}")
                        if 'is_cointegrated' in result:
                            cointegration = "cointegrated" if result['is_cointegrated'] else "not cointegrated"
                            details_parts.append(cointegration)
                        details = " | ".join(details_parts)
                else:
                    status = "PASS"
                    details = str(result)

                report.append(f"{test_name:<40} {status:<10} {details}")

        # Now handle advanced results
        if 'advanced_validation' in results:
            report.append("\n◆ ADVANCED VALIDATION")
            for test_name, result in results['advanced_validation'].items():
                status = "PASS" if isinstance(result, dict) and result.get('p_value', 1) < 0.05 else "WARNING"
                details_parts = []
                if isinstance(result, dict):
                    if 'p_value' in result:
                        details_parts.append(f"p={result['p_value']:.4f}")
                    if 'statistic' in result:
                        details_parts.append(f"stat={result['statistic']:.2f}")
                    if 'notes' in result:
                        details_parts.append(result['notes'])
                details = " | ".join(details_parts) if details_parts else str(result)
                report.append(f"{test_name:<40} {status:<10} {details}")

        return "\n".join(report)


# Singleton validator instance
validator = DiamondValidator()

# Module-level functions for pipeline integration
def validate_cotations(df: pd.DataFrame) -> Dict[str, Any]:
    return validator.validate_cotations(df)

def validate_indices(df: pd.DataFrame) -> Dict[str, Any]:
    return validator.validate_indices(df)

def generate_validation_report(results: dict) -> str:
    return validator.generate_report(results)