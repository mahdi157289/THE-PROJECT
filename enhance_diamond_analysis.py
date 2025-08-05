#!/usr/bin/env python3
"""
Enhanced Diamond Analysis - Unlocking Full Script Potential
==========================================================

This script implements ALL the advanced features from your advanced_validator.py
that are currently not being used in your pipeline.

FEATURES TO ADD:
1. Enhanced Cointegration Analysis (Johansen + Engle-Granger)
2. Market Regime Detection (Bull/Bear/Sideways)
3. Advanced Risk Metrics (VaR, Expected Shortfall, Drawdown)
4. Structural Break Detection (Chow test, CUSUM)
5. Enhanced Trading Strategies (Dynamic thresholds)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import json
from pathlib import Path
import sys
import os

# Add the src directory to path
sys.path.append('src')

from diamond_layer1.advanced_validator import (
    AdvancedMarketValidator, 
    calculate_risk_metrics, 
    detect_structural_breaks
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedDiamondAnalysis:
    """
    Enhanced analysis using ALL features from advanced_validator.py
    """
    
    def __init__(self):
        self.advanced_validator = AdvancedMarketValidator()
        self.engine = create_engine('postgresql://postgres:Mahdi1574$@localhost:5432/pfe_database')
        
    def load_market_data(self):
        """Load market data from golden layer"""
        try:
            # Load from golden layer with correct column names
            cotations_query = "SELECT * FROM golden_cotations ORDER BY seance"
            indices_query = "SELECT * FROM golden_indices ORDER BY seance"
            
            cotations = pd.read_sql(cotations_query, self.engine)
            indices = pd.read_sql(indices_query, self.engine)
            
            logger.info(f"Loaded {len(cotations)} cotations and {len(indices)} indices records")
            return cotations, indices
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            return None, None
    
    def run_enhanced_cointegration_analysis(self, indices: pd.DataFrame):
        """Run enhanced cointegration analysis using ALL methods"""
        logger.info("Running enhanced cointegration analysis...")
        
        results = {}
        stock_columns = [col for col in indices.columns if col not in ['seance', 'lib_indice']]
        
        # Use the actual price columns from the data
        price_columns = [col for col in indices.columns if 'indice' in col.lower() and ('jour' in col.lower() or 'veille' in col.lower() or 'haut' in col.lower() or 'bas' in col.lower() or 'ouv' in col.lower())]
        
        for i, idx1 in enumerate(price_columns[:5]):  # Limit to first 5 price columns
            for idx2 in price_columns[i+1:6]:  # Compare with next 5
                if idx1 in indices.columns and idx2 in indices.columns:
                    try:
                        series1 = pd.to_numeric(indices[idx1], errors='coerce').dropna()
                        series2 = pd.to_numeric(indices[idx2], errors='coerce').dropna()
                        
                        if len(series1) > 50 and len(series2) > 50:
                            # Align series
                            aligned = pd.DataFrame({idx1: series1, idx2: series2}).dropna()
                            if len(aligned) > 50:
                                coint_result = self.advanced_validator.enhanced_cointegration_test(
                                    aligned[idx1], aligned[idx2]
                                )
                                results[f'{idx1}_vs_{idx2}_cointegration'] = coint_result
                    except Exception as e:
                        logger.warning(f"Failed to process cointegration for {idx1} vs {idx2}: {str(e)}")
                        continue
        
        logger.info(f"Completed enhanced cointegration analysis for {len(results)} pairs")
        return results
    
    def run_market_regime_analysis(self, cotations: pd.DataFrame):
        """Run market regime detection for all stocks"""
        logger.info("Running market regime detection...")
        
        results = {}
        stock_columns = [col for col in cotations.columns if col not in ['seance', 'groupe', 'code']]
        
        # Use price columns
        price_columns = [col for col in stock_columns if 'prix' in col.lower() or 'close' in col.lower() or 'cloture' in col.lower() or 'ouverture' in col.lower() or 'valeur' in col.lower()]
        
        for stock in price_columns[:10]:  # Limit to top 10 stocks for performance
            if stock in cotations.columns:
                # Convert to numeric and handle errors
                try:
                    prices = pd.to_numeric(cotations[stock], errors='coerce').dropna()
                    if len(prices) > 120:
                        returns = prices.pct_change().dropna()
                        if len(returns) > 120:  # Need at least 120 days for regime detection
                            regime_result = self.advanced_validator.market_regime_detection(returns, window=60)
                            results[f'{stock}_regime_detection'] = regime_result
                except Exception as e:
                    logger.warning(f"Failed to process {stock}: {str(e)}")
                    continue
        
        logger.info(f"Completed market regime detection for {len(results)} stocks")
        return results
    
    def run_advanced_risk_analysis(self, cotations: pd.DataFrame):
        """Run advanced risk metrics for all stocks"""
        logger.info("Running advanced risk analysis...")
        
        results = {}
        stock_columns = [col for col in cotations.columns if col not in ['seance', 'groupe', 'code']]
        
        # Use price columns
        price_columns = [col for col in stock_columns if 'prix' in col.lower() or 'close' in col.lower() or 'cloture' in col.lower() or 'ouverture' in col.lower() or 'valeur' in col.lower()]
        
        for stock in price_columns[:15]:  # Limit to top 15 stocks
            if stock in cotations.columns:
                try:
                    prices = pd.to_numeric(cotations[stock], errors='coerce').dropna()
                    if len(prices) > 30:
                        returns = prices.pct_change().dropna()
                        if len(returns) > 30:
                            risk_result = calculate_risk_metrics(returns)
                            results[f'{stock}_risk_metrics'] = risk_result
                except Exception as e:
                    logger.warning(f"Failed to process {stock}: {str(e)}")
                    continue
        
        logger.info(f"Completed advanced risk analysis for {len(results)} stocks")
        return results
    
    def run_structural_break_analysis(self, cotations: pd.DataFrame):
        """Run structural break detection for major stocks"""
        logger.info("Running structural break detection...")
        
        results = {}
        stock_columns = [col for col in cotations.columns if col not in ['seance', 'groupe', 'code']]
        
        # Use price columns
        price_columns = [col for col in stock_columns if 'prix' in col.lower() or 'close' in col.lower() or 'cloture' in col.lower() or 'ouverture' in col.lower() or 'valeur' in col.lower()]
        
        for stock in price_columns[:6]:  # Top 6 stocks
            if stock in cotations.columns:
                try:
                    prices = pd.to_numeric(cotations[stock], errors='coerce').dropna()
                    if len(prices) > 100:
                        break_result = detect_structural_breaks(prices)
                        results[f'{stock}_structural_breaks'] = break_result
                except Exception as e:
                    logger.warning(f"Failed to process {stock}: {str(e)}")
                    continue
        
        logger.info(f"Completed structural break detection for {len(results)} stocks")
        return results
    
    def run_enhanced_trading_strategies(self, cotations: pd.DataFrame):
        """Run enhanced trading strategies with dynamic thresholds"""
        logger.info("Running enhanced trading strategies...")
        
        results = {}
        stock_columns = [col for col in cotations.columns if col not in ['seance', 'groupe', 'code']]
        
        # Use price columns
        price_columns = [col for col in stock_columns if 'prix' in col.lower() or 'close' in col.lower() or 'cloture' in col.lower() or 'ouverture' in col.lower() or 'valeur' in col.lower()]
        
        # Create market index as benchmark (use first price column as benchmark)
        if len(price_columns) > 0:
            try:
                benchmark_prices = pd.to_numeric(cotations[price_columns[0]], errors='coerce').dropna()
                benchmark = benchmark_prices.pct_change().dropna()
                
                for stock in price_columns[1:11]:  # Top 10 stocks (excluding benchmark)
                    if stock in cotations.columns:
                        try:
                            stock_prices = pd.to_numeric(cotations[stock], errors='coerce').dropna()
                            stock_returns = stock_prices.pct_change().dropna()
                            
                            # Align with benchmark
                            aligned = pd.DataFrame({
                                'stock': stock_returns,
                                'benchmark': benchmark
                            }).dropna()
                            
                            if len(aligned) > 60:
                                # Enhanced correlation strategy with multiple windows
                                for window in [30, 60, 90]:
                                    strategy_result = self.advanced_validator.rolling_correlation_strategy(
                                        aligned['stock'], 
                                        aligned['benchmark'], 
                                        window=window, 
                                        threshold=1.5
                                    )
                                    results[f'{stock}_correlation_strategy_{window}d'] = strategy_result
                        except Exception as e:
                            logger.warning(f"Failed to process {stock}: {str(e)}")
                            continue
            except Exception as e:
                logger.warning(f"Failed to process benchmark: {str(e)}")
        
        logger.info(f"Completed enhanced trading strategies for {len(results)} combinations")
        return results
    
    def save_enhanced_results(self, all_results: dict):
        """Save enhanced results to database and files"""
        logger.info("Saving enhanced results...")
        
        # Save to files
        output_dir = Path('data/diamond')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save as JSON
        results_file = output_dir / f'enhanced_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        # Save to database
        self.save_to_diamond_advanced_results(all_results)
        
        logger.info(f"Enhanced results saved to {results_file}")
        return results_file
    
    def save_to_diamond_advanced_results(self, results: dict):
        """Save enhanced results to diamond_advanced_results table"""
        try:
            execution_date = datetime.now().date()
            
            with self.engine.connect() as conn:
                for test_name, result in results.items():
                    if isinstance(result, dict) and 'error' not in result:
                        # Determine category and model type
                        if 'cointegration' in test_name:
                            category = 'Market Analysis'
                            model_type = 'Cointegration'
                        elif 'regime' in test_name:
                            category = 'Market Analysis'
                            model_type = 'Regime Detection'
                        elif 'risk' in test_name:
                            category = 'Risk Management'
                            model_type = 'Risk Metrics'
                        elif 'structural' in test_name:
                            category = 'Market Analysis'
                            model_type = 'Structural Breaks'
                        elif 'correlation' in test_name:
                            category = 'Trading Strategies'
                            model_type = 'Correlation Strategy'
                        else:
                            category = 'Advanced Analysis'
                            model_type = 'Statistical'
                        
                        insert_sql = """
                        INSERT INTO diamond_advanced_results 
                        (test_category, test_name, execution_date, model_type, parameters, 
                         performance_metrics, raw_output, notes)
                        VALUES (:test_category, :test_name, :execution_date, :model_type, 
                                :parameters, :performance_metrics, :raw_output, :notes)
                        """
                        
                        conn.execute(text(insert_sql), {
                            'test_category': category,
                            'test_name': test_name,
                            'execution_date': execution_date,
                            'model_type': model_type,
                            'parameters': json.dumps({}),
                            'performance_metrics': json.dumps(result.get('performance_metrics', {})),
                            'raw_output': json.dumps(result),
                            'notes': f'Enhanced analysis: {test_name}'
                        })
                
                conn.commit()
                logger.info("Enhanced results saved to database")
                
        except Exception as e:
            logger.error(f"Failed to save enhanced results to database: {str(e)}")
    
    def run_full_enhanced_analysis(self):
        """Run the complete enhanced analysis"""
        logger.info("Starting full enhanced analysis...")
        
        # Load data
        cotations, indices = self.load_market_data()
        if cotations is None or indices is None:
            logger.error("Failed to load data")
            return False
        
        # Run all enhanced analyses
        all_results = {}
        
        # 1. Enhanced Cointegration Analysis
        coint_results = self.run_enhanced_cointegration_analysis(indices)
        all_results.update(coint_results)
        
        # 2. Market Regime Detection
        regime_results = self.run_market_regime_analysis(cotations)
        all_results.update(regime_results)
        
        # 3. Advanced Risk Analysis
        risk_results = self.run_advanced_risk_analysis(cotations)
        all_results.update(risk_results)
        
        # 4. Structural Break Detection
        break_results = self.run_structural_break_analysis(cotations)
        all_results.update(break_results)
        
        # 5. Enhanced Trading Strategies
        strategy_results = self.run_enhanced_trading_strategies(cotations)
        all_results.update(strategy_results)
        
        # Save results
        results_file = self.save_enhanced_results(all_results)
        
        logger.info(f"Enhanced analysis completed! Generated {len(all_results)} new analyses")
        logger.info(f"Results saved to: {results_file}")
        
        return True

def main():
    """Main function to run enhanced analysis"""
    logger.info("Starting Enhanced Diamond Analysis...")
    
    analyzer = EnhancedDiamondAnalysis()
    success = analyzer.run_full_enhanced_analysis()
    
    if success:
        logger.info("✅ Enhanced analysis completed successfully!")
        logger.info("🎯 You now have access to ALL advanced features:")
        logger.info("   - Enhanced Cointegration (Johansen + Engle-Granger)")
        logger.info("   - Market Regime Detection (Bull/Bear/Sideways)")
        logger.info("   - Advanced Risk Metrics (VaR, Expected Shortfall)")
        logger.info("   - Structural Break Detection (Chow test, CUSUM)")
        logger.info("   - Enhanced Trading Strategies (Dynamic thresholds)")
    else:
        logger.error("❌ Enhanced analysis failed!")

if __name__ == "__main__":
    main() 