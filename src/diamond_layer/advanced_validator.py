"""Advanced statistical validation and modeling"""
import pandas as pd
import numpy as np
import logging
from scipy import stats
from arch import arch_model
from statsmodels.tsa.api import VAR

logger = logging.getLogger('AdvancedValidator')

class AdvancedMarketValidator:
    def __init__(self, min_obs=30):
        self.min_obs = min_obs
        self.logger = logger
    
    def student_t_fit_test(self, series: pd.Series) -> dict:
        """Fit Student's t-distribution and perform KS test"""
        series_clean = series.dropna()
        if len(series_clean) < self.min_obs:
            self.logger.warning(f"Insuffient observations for t-test: {len(series_clean)} < {self.min_obs}")
            return {'error': f"Need {self.min_obs} observations, got {len(series_clean)}"}
        
        try:
            # Fit Student's t-distribution
            params = stats.t.fit(series_clean)
            df, loc, scale = params
            
            # Kolmogorov-Smirnov test
            D, p_value = stats.kstest(series_clean, 't', args=params)
            
            return {
                'test': 'Student-t KS Test',
                'degrees_freedom': df,
                'location': loc,
                'scale': scale,
                'statistic': D,
                'p_value': p_value,
                'is_good_fit': p_value > 0.05
            }
        except Exception as e:
            self.logger.error(f"Student-t test failed: {str(e)}")
            return {'error': str(e)}
    
    def garch_volatility_analysis(self, returns: pd.Series, p=1, q=1) -> dict:
        """Fit GARCH model to return series"""
        returns_clean = returns.dropna()
        if len(returns_clean) < 100:
            self.logger.warning(f"Insuffient data for GARCH: {len(returns_clean)} < 100")
            return {'error': f"Need 100+ observations, got {len(returns_clean)}"}
        
        try:
            # Scale returns for numerical stability
            scaled_returns = returns_clean * 100
            model = arch_model(scaled_returns, mean='Zero', vol='GARCH', p=p, q=q)
            results = model.fit(disp='off')
            
            return {
                'model': f'GARCH({p},{q})',
                'params': results.params.to_dict(),
                'bic': results.bic,
                'persistence': results.arch_model.volatility.persistence(params=results.params),
                'conditional_volatility': results.conditional_volatility.tolist()
            }
        except Exception as e:
            self.logger.error(f"GARCH modeling failed: {str(e)}")
            return {'error': str(e)}
    
    def rolling_correlation_strategy(self, series1: pd.Series, series2: pd.Series, 
                                   window=30, threshold=1.5) -> dict:
        """Generate trading signals based on rolling correlation"""
        aligned = pd.DataFrame({'s1': series1, 's2': series2}).dropna()
        if len(aligned) < window:
            self.logger.warning(f"Window too large for data: {window} > {len(aligned)}")
            return {'error': f"Window {window} larger than data length {len(aligned)}"}
        
        corr = aligned['s1'].rolling(window).corr(aligned['s2'])
        z_scores = (corr - corr.mean()) / corr.std()
        
        # Generate signals
        signals = pd.Series(0, index=z_scores.index)
        signals[z_scores > threshold] = 1    # Overbought signal
        signals[z_scores < -threshold] = -1   # Oversold signal
        
        return {
            'window': window,
            'threshold': threshold,
            'signals': signals.dropna().tolist(),
            'z_scores': z_scores.dropna().tolist()
        }
    
    def vector_autoregression(self, data: pd.DataFrame, maxlags=5) -> dict:
        """Fit VAR model to multivariate time series"""
        if len(data) < maxlags * 2:
            self.logger.warning(f"Insuffient data for VAR: {len(data)} < {maxlags*2}")
            return {'error': f"Need at least {maxlags*2} observations"}
        
        try:
            model = VAR(data)
            results = model.fit(maxlags=maxlags, ic='aic')
            
            return {
                'selected_lags': results.k_ar,
                'aic': results.aic,
                'bic': results.bic,
                'params': results.params.to_dict(),
                'granger_causality': results.test_causality().summary().as_text()
            }
        except Exception as e:
            self.logger.error(f"VAR modeling failed: {str(e)}")
            return {'error': str(e)}