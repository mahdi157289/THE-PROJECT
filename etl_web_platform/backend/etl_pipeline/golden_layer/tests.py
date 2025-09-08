"""
Statistical tests for BVMT market data validation
Core tests used across both cotations and indices analysis
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.stats import anderson
from statsmodels.tsa.stattools import adfuller, grangercausalitytests, coint
import logging
from .golden_features_creation import STOCK_SECTOR_MAP
def configure_logger():
    """Set up standardized logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('golden_tests.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('GoldenTests')

logger = configure_logger()

def normality_test(series: pd.Series) -> dict:
    """Test for normality with enhanced logging"""
    try:
        
        result = anderson(series.dropna())
        return {
            'test': 'Anderson-Darling',
            'statistic': result.statistic,
            'critical_values': result.critical_values,
            'significance_level': result.significance_level
        }
    except Exception as e:
        logger.error(f"Shapiro-Wilk failed: {str(e)}")
        raise

def levene_test(*groups: pd.Series) -> dict:
    """Compare variance across groups with sector-aware logging"""
    group_names = [f"Group_{i}" for i in range(len(groups))]  # Default names
    if hasattr(groups[0], 'name'):  # Use series names if available
        group_names = [g.name for g in groups]
    
    try:
        stat, p_value = stats.levene(*[g.dropna() for g in groups])
        logger.info(f"Levene's test ({' vs '.join(group_names)}): p={p_value:.4f}")
        return {
            'test': 'Levene',
            'groups': group_names,
            'statistic': stat,
            'p_value': p_value,
            'equal_variance': p_value > 0.05
        }
    except Exception as e:
        logger.error(f"Levene's test failed: {str(e)}")
        raise

def adf_test(series: pd.Series, regression: str = 'c') -> dict:
    """Stationarity test with BVMT-specific critical values"""
    try:
        result = adfuller(series.dropna(), regression=regression)
        logger.debug(f"ADF on {series.name}: ADF={result[0]:.2f}, p={result[1]:.4f}")
        return {
            'test': 'ADF',
            'statistic': result[0],
            'p_value': result[1],
            'critical_values': result[4],
            'is_stationary': result[1] < 0.05
        }
    except Exception as e:
        logger.error(f"ADF test failed: {str(e)}")
        raise

def granger_causality_test(cause: pd.Series, effect: pd.Series, maxlag: int = 2) -> dict:
    """Causality detection between series"""
    try:
        df = pd.DataFrame({'cause': cause, 'effect': effect}).dropna()
        results = grangercausalitytests(df[['effect', 'cause']], maxlag=maxlag, verbose=False)
        
        test_results = {}
        for lag in range(1, maxlag + 1):
            p_value = results[lag][0]['ssr_ftest'][1]
            test_results[lag] = {
                'p_value': p_value,
                'causality': p_value < 0.05
            }
            logger.info(f"Granger {cause.name}->{effect.name} (lag {lag}): p={p_value:.4f}")
        return test_results
    except Exception as e:
        logger.error(f"Granger test failed: {str(e)}")
        raise

def cointegration_test(series1: pd.Series, series2: pd.Series) -> dict:
    """Enhanced with data validation"""
    try:
        # Ensure equal length
        min_length = min(len(series1.dropna()), len(series2.dropna()))
        if min_length < 30:
            return {
                'error': f"Insufficient data (n={min_length} < 30)",
                'is_cointegrated': None
            }
            
        # Align series
        aligned = pd.DataFrame({'s1': series1, 's2': series2}).dropna()
        if len(aligned) < 30:
            return {
                'error': f"After alignment n={len(aligned)} < 30",
                'is_cointegrated': None
            }
            
        score, p_value, _ = coint(aligned['s1'], aligned['s2'])
        return {
            'statistic': score,
            'p_value': p_value,
            'is_cointegrated': p_value < 0.05,
            'n_observations': len(aligned)
        }
        
    except Exception as e:
        return {
            'error': str(e),
            'is_cointegrated': None
        }

def compute_sector_beta(stock_returns: pd.Series, sector_returns: pd.Series) -> float:
    """Sector sensitivity measurement"""
    try:
        aligned = pd.DataFrame({'stock': stock_returns, 'sector': sector_returns}).dropna()
        if len(aligned) < 2:
            return np.nan
        cov = np.cov(aligned['stock'], aligned['sector'])
        beta = cov[0, 1] / cov[1, 1]
        logger.debug(f"Beta {stock_returns.name} to {sector_returns.name}: {beta:.2f}")
        return beta
    except Exception as e:
        logger.error(f"Beta computation failed: {str(e)}")
        raise