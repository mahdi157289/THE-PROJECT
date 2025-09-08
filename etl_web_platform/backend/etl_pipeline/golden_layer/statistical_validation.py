"""
Statistical validation for golden layer features
Complete validation pipeline with detailed reporting
"""

import pandas as pd
import numpy as np
from . import tests
import logging
from .golden_features_creation import SectorClassifier
from .golden_features_creation import STOCK_SECTOR_MAP
from typing import Set, Dict, List
logger = logging.getLogger('GoldenValidation')
class SectorClassifier:
    def __init__(self):
        # Existing stock-to-sector mapping
        self.stock_to_sector = STOCK_SECTOR_MAP
        
        # All valid sectors that should have stocks
        self.valid_sectors = set(STOCK_SECTOR_MAP.values())
        
        # Special indices that shouldn't be treated as sectors
        self.pure_indices = {'TUNINDEX', 'TUNINDEX20', 'PX1', 'TUN20'}
        
        # Will track sectors found in data but without stocks
        self.orphan_sectors = set()

    def validate_mappings(self, available_indices: Set[str]) -> bool:
        """Returns True if all mapped sectors exist in data"""
        self.orphan_sectors = set(available_indices) - self.valid_sectors - self.pure_indices
        if self.orphan_sectors:
            logger.warning(f"Sectors without stock mappings: {self.orphan_sectors}")
        return len(self.orphan_sectors) == 0

    def get_stocks_for_sector(self, sector: str) -> List[str]:
        """Get all stocks belonging to a sector"""
        return [stock for stock, s in self.stock_to_sector.items() if s == sector]

    def is_pure_index(self, index_name: str) -> bool:
        """Check if an index should be excluded from sector processing"""
        return index_name in self.pure_indices
    
def generate_validation_report(test_results: dict) -> str:
    """Format test results for human-readable reporting"""
    report = ["[REPORT] VALIDATION REPORT"]  # Replaced 📊
    for test_name, result in test_results.items():
        report.append(f"\n[TEST] {test_name}")  # Replaced 🔍
        if 'p_value' in result:
            sig = "PASS" if result['p_value'] < 0.05 else "FAIL"  # Replaced ✓/✗
            report.append(f"   p-value: {result['p_value']:.4f} [{sig}]")
        if 'statistic' in result:
            report.append(f"   Statistic: {result['statistic']:.4f}")
        if 'is_cointegrated' in result:
            status = "COINTEGRATED" if result['is_cointegrated'] else "NOT COINTEGRATED"
            report.append(f"   Status: {status}")
    return "\n".join(report)

def validate_cotations(df: pd.DataFrame, sector_indices: dict) -> dict:
    """Full validation suite for cotations data with error handling"""
    logger.info("Starting cotations validation")
    results = {}
    classifier = SectorClassifier()
    # First validate all required sector indices exist
    if not classifier.validate_mappings(sector_indices.keys()):
        results['mapping_error'] = "Missing sector indices"
    # Skip if no valid sectors
    if not any(sector in classifier.valid_sectors for sector in sector_indices):
        logger.warning("No valid sectors for validation")
        return results    
    # Calculate returns for all stocks
    returns = df.groupby('valeur')['cloture'].pct_change()
    
    try:
        # Price Distribution Tests
        with np.errstate(all='ignore'):  # Suppress runtime warnings
            try:
                results['normality'] = tests.normality_test(returns.dropna())
            except Exception as e:
                logger.error(f"Normality test failed: {e}")
                results['normality'] = {'error': str(e)}

            # Sector Volatility Comparison
            try:
                # Sector analysis using the pre-calculated returns
                sector_vols = {}
                for sector in sector_indices:
                    stocks = classifier.get_stocks_for_sector(sector)
                    if not stocks:
                        continue
                    sector_data = returns[df['valeur'].isin(stocks)]
                    if len(sector_data) > 10:  # Minimum data threshold
                        sector_vols[sector] = sector_data
                        
                if sector_vols:
                    results['sector_volatility'] = tests.levene_test(*sector_vols.values())
                else:
                    logger.warning("Insufficient data for sector volatility test")
            except Exception as e:
                        logger.error(f"Sector volatility test failed: {e}")
                

            # Stationarity
            try:
                sample_stocks = df['valeur'].unique()[:5]  # Test first 5 stocks
                results['stationarity'] = {
                    stock: tests.adf_test(df[df['valeur'] == stock]['cloture'])
                    for stock in sample_stocks
                }
            except Exception as e:
                logger.error(f"Stationarity test failed: {e}")
                results['stationarity'] = {'error': str(e)}

    except Exception as e:
        logger.critical(f"Validation failed: {e}", exc_info=True)
        results['critical_error'] = str(e)

    logger.info(generate_validation_report(results))
    return results

def validate_indices(indices_df: pd.DataFrame) -> dict:
    """Comprehensive indices validation with error handling"""
    logger.info("Starting indices validation")
    results = {}
    try:
        # ✅ Define market_index BEFORE using it
        classifier = SectorClassifier()  # <-- ADD THIS
        market_index = indices_df[indices_df['lib_indice'] == 'TUNINDEX'].set_index('seance')['indice_jour']
        

        # Cointegration tests
        try:
            # Cointegration with Market Index
            for sector in indices_df['lib_indice'].unique():
                if classifier.is_pure_index(sector): continue
                sector_series = indices_df[indices_df['lib_indice'] == sector].set_index('seance')['indice_jour']
                # NEW: Date alignment and validation
                common_dates = sector_series.index.intersection(market_index.index)
                if len(common_dates) < 30:  # Minimum 30 days for meaningful test
                    logger.warning(f"Insufficient overlapping dates ({len(common_dates)}) for {sector}")
                    continue
                    
                aligned_sector = sector_series.loc[common_dates]
                aligned_market = market_index.loc[common_dates]
                
                results[f"{sector}_cointegration"] = tests.cointegration_test(
                    aligned_sector, 
                    aligned_market
                )
                # Debug alignment print
                print(f"\nSector: {sector}")
                print(f"  Start date: {sector_series.index.min()} - End date: {sector_series.index.max()}")
                print(f"  Market start: {market_index.index.min()} - Market end: {market_index.index.max()}")
                print(f"  Common dates: {len(sector_series.index.intersection(market_index.index))}")
        except Exception as e:
                logger.error(f"Cointegration test failed: {e}")
                results['cointegration_error'] = str(e)

        # Granger Causality
        try:
            financial_sectors = ['TUNBANQ', 'TUNASS', 'TUNFIN']
            for i, sector1 in enumerate(financial_sectors):
                for sector2 in financial_sectors[i+1:]:
                    s1 = indices_df[indices_df['lib_indice'] == sector1].set_index('seance')['indice_jour']
                    s2 = indices_df[indices_df['lib_indice'] == sector2].set_index('seance')['indice_jour']
                    results[f"{sector1}_to_{sector2}_granger"] = tests.granger_causality_test(s1, s2)
        except Exception as e:
            logger.error(f"Granger causality test failed: {e}")
            results['granger_error'] = str(e)

    except Exception as e:
        logger.critical(f"Indices validation failed: {e}", exc_info=True)
        results['critical_error'] = str(e)

    logger.info(generate_validation_report(results))
    return results