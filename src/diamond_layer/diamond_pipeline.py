"""
Diamond Layer - Statistical Validation Pipeline
Loads from Golden tables, validates, and stores results
"""

from pathlib import Path
import pandas as pd
import dask.dataframe as dd
import logging
from datetime import datetime
from typing import Dict, Any
from src.diamond_layer.database_manager import DiamondDatabaseManager
from src.golden_layer.golden_saver import GoldenDataSaver
from src.utils.db_connection import DatabaseConnector
from . import tests
from .advanced_validator import AdvancedMarketValidator
from .deep_models import MarketModelBuilder
class DiamondLayer:
    def __init__(self, enable_advanced_models=True):
        self.logger = self._configure_logger()
        self.saver = GoldenDataSaver(
            engine=DatabaseConnector().engine,
            logger=self.logger
        )
        self.db_manager = DiamondDatabaseManager(
            engine=DatabaseConnector().engine,
            logger=self.logger
        )
        self.enable_advanced_models = enable_advanced_models
        
        self.model_builder = MarketModelBuilder()
        self.advanced_validator = AdvancedMarketValidator()
        self.logger.phase = 'SYSTEM'
        self.start_time = None
    def _log_data_stats(self, cotations: pd.DataFrame, indices: pd.DataFrame):
        """Logs key statistics for debugging."""
        self.logger.debug(
            f"Cotations: {len(cotations)} rows, {cotations['valeur'].nunique()} unique stocks",
            extra={'phase': 'DATA_LOADING'}
        )
        self.logger.debug(
            f"Indices: {len(indices)} rows, {indices['lib_indice'].nunique()} unique indices",
            extra={'phase': 'DATA_LOADING'}
        )    
    def run_validation(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> Dict[str, Any]:
        self._log_phase_start("VALIDATION")
        try:
            # Base validation
            base_results = {
                'cotations': tests.validate_cotations(cotations),
                'indices': tests.validate_indices(indices)
            }
            
            # Advanced validation if enabled
            advanced_results = {}
            if self.enable_advanced_models:
                advanced_results = self._run_advanced_validation(cotations, indices)
            
            # Merge results
            results = {**base_results, **advanced_results}
            
            # Print report
            print(tests.generate_validation_report(results))
            self._log_phase_end(True, f"Generated {len(results)} test results")
            return results
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise
    
    def _run_advanced_validation(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> Dict[str, Any]:
        """Run advanced statistical tests and models"""
        self.logger.info("Starting advanced validation", extra={'phase': 'ADVANCED_VALIDATION'})
        results = {}
        
        try:
            # 1. Advanced distribution tests
            sample_stock = cotations['valeur'].iloc[0]
            stock_data = cotations[cotations['valeur'] == sample_stock]
            returns = stock_data['cloture'].pct_change().dropna()
            
            results['t_distribution'] = self.advanced_validator.student_t_fit_test(returns)
            results['garch_volatility'] = self.advanced_validator.garch_volatility_analysis(returns)
            
            # 2. Correlation strategy for non-cointegrated pairs
            indices_pivot = indices.pivot(index='seance', columns='lib_indice', values='indice_jour')
            
            if 'TUNINDEX' in indices_pivot:
                market_series = indices_pivot['TUNINDEX']
                
                # Apply correlation strategy to non-cointegrated indices
                non_cointegrated_indices = [
                    'TUNBANQ', 'TUNFIN', 'TUNCONS', 'TUNDIS', 'TUNBASE'
                    # Add others that failed cointegration test
                ]
                
                for idx in non_cointegrated_indices:
                    if idx in indices_pivot:
                        results[f'{idx}_correlation_strategy'] = self.advanced_validator.rolling_correlation_strategy(
                            market_series, indices_pivot[idx], window=30, threshold=1.5
                        )
            
            # 3. VAR model for sector indices
            sector_indices = indices_pivot[['TUNBANQ', 'TUNASS', 'TUNFIN']].dropna()
            if not sector_indices.empty:
                results['var_model'] = self.advanced_validator.vector_autoregression(sector_indices)
            
            return results
        except Exception as e:
            self.logger.error(f"Advanced validation failed: {str(e)}")
            return {'advanced_validation_error': str(e)}
    
    def run_model_training(self, cotations: pd.DataFrame) -> dict:
        """Train deep learning models"""
        if not self.enable_advanced_models:
            return {}
            
        self._log_phase_start("MODEL_TRAINING")
        try:
            # Prepare data
            sample_stock = cotations['valeur'].iloc[0]
            stock_data = cotations[cotations['valeur'] == sample_stock]
            prices = stock_data['cloture'].values
            
            # Create sequences
            X, y = self.model_builder._create_sequences(prices)
            X = X.reshape((X.shape[0], X.shape[1], 1))
            
            # Train models
            results = {}
            cnn_lstm = self.model_builder.build_cnn_lstm((X.shape[1], 1))
            results['cnn_lstm'] = self.model_builder.train_model(cnn_lstm, X, y)
            
            transformer = self.model_builder.build_transformer((X.shape[1], 1))
            results['transformer'] = self.model_builder.train_model(transformer, X, y)
            
            self._log_phase_end(True, "Trained deep learning models")
            return results
        except Exception as e:
            self._log_phase_end(False, str(e))
            return {'model_training_error': str(e)}
            
    def initialize_database(self):
        """Fixed database initialization"""
        self._log_phase_start("DB_INITIALIZATION")
        try:
            # First ensure the saver's schema is initialized
            if hasattr(self.saver, 'initialize_schema'):
                self.saver.initialize_schema()
            
            # Then initialize pipeline-specific tables
            self.db_manager.execute_ddl([
                self._get_base_schema(),
                self._get_metrics_schema()
            ])
            self._log_phase_end(True, "Database initialized")
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise
    def _configure_logger(self):
        """Set up comprehensive logging with phase handling"""
        logger = logging.getLogger('DiamondLayer')
        logger.setLevel(logging.INFO)
        
        class PhaseFormatter(logging.Formatter):
            def format(self, record):
                if not hasattr(record, 'phase'):
                    record.phase = 'SYSTEM'  # Default phase
                return super().format(record)
        
        formatter = PhaseFormatter(
            '%(asctime)s | %(levelname)-8s | %(phase)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Remove existing handlers to avoid duplicates
        if logger.handlers:
            logger.handlers.clear()
            
        fh = logging.FileHandler('diamond_pipeline.log', encoding='utf-8')
        fh.setFormatter(formatter)
        
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

    def _log_phase_start(self, phase_name: str):
        """Standardized phase logging with timestamps."""
        self.phase = phase_name
        self.start_time = datetime.now()
        self.logger.info(
            f"START: {phase_name}",
            extra={'phase': phase_name, 'action': 'begin'}
        )

    def _log_phase_end(self, success: bool, details: str = None):
        """Logs phase completion with duration."""
        duration = (datetime.now() - self.start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(
            f"END: {self.phase} ({duration:.2f}s) | {status} | {details}",
            extra={
                'phase': self.phase,
                'status': status.lower(),
                'duration_sec': duration,
                'action': 'complete'
            }
        )

    def load_from_golden(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load data from golden tables"""
        self._log_phase_start("DATA_LOADING")
        try:
            engine = DatabaseConnector().engine
            
            cotations = pd.read_sql_table('golden_cotations', engine)
            indices = pd.read_sql_table('golden_indices', engine)
            
            self._log_phase_end(True, f"Loaded {len(cotations)} cotations, {len(indices)} indices")
            return cotations, indices
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise

    def run_validation(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> Dict[str, Any]:
        """Run all validation tests"""
        self._log_phase_start("VALIDATION")
        try:
            results = {
                'cotations': tests.validate_cotations(cotations),
                'indices': tests.validate_indices(indices)
            }
            # Print detailed results to terminal
            print(tests.generate_validation_report(results))
            self._log_phase_end(True, f"Generated {len(results)} test results")
            return results
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise
    def _prepare_metrics(self, results: Dict[str, Any]) -> pd.DataFrame:
        """Properly structure validation metrics"""
        rows = []
        
        def _flatten_results(prefix, data):
            if isinstance(data, dict):
                for k, v in data.items():
                    new_prefix = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, dict) and any(key in v for key in ['statistic', 'p_value', 'is_cointegrated']):
                        # This is a test result
                        row = {
                            'test_name': new_prefix,
                            'execution_time': datetime.now(),
                            'status': 'completed' if 'error' not in v else 'failed'
                        }
                        row.update({nk: nv for nk, nv in v.items() 
                                if nk in ['statistic', 'p_value', 'is_cointegrated']})
                        rows.append(row)
                    else:
                        _flatten_results(new_prefix, v)
        
        _flatten_results('', results)
        return pd.DataFrame(rows)

    def save_results(self, validation_results: Dict[str, Any]):
        """Fixed result saving"""
        self._log_phase_start("RESULT_SAVING")
        try:
            # Convert results to DataFrame
            metrics_df = self._prepare_metrics(validation_results)
            
            # Ensure database is ready
            if hasattr(self.saver, 'initialize_schema'):
                self.saver.initialize_schema()
            
            # Save to parquet
            output_dir = Path('data/diamond')
            output_dir.mkdir(parents=True, exist_ok=True)
            metrics_df.to_parquet(output_dir / 'validation_metrics.parquet')
            
            # Save to database
            if hasattr(self.saver, 'save_metrics'):
                self.saver.save_metrics(metrics_df.to_dict('records'))
            
            self._log_phase_end(True, f"Saved {len(metrics_df)} metrics")
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise
    def _get_base_schema(self) -> str:
        """Base schema for diamond layer tables"""
        return """
        CREATE TABLE IF NOT EXISTS diamond_metrics (
            metric_id SERIAL PRIMARY KEY,
            test_name VARCHAR(255) NOT NULL,
            execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            details JSONB
        );
        """

    def _get_metrics_schema(self) -> str:
        """Extended metrics schema with additional columns"""
        return """
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            statistic_value DOUBLE PRECISION;
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            p_value DOUBLE PRECISION;
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            test_duration INTERVAL;
        """
    def _validate_indices(self, df: pd.DataFrame) -> dict:
        """Runs all statistical tests on indices data with market-aware checks."""
        results = {}
        validator = tests.DiamondValidator()
        try:
        # Prepare data first (add sector mapping if missing)
            if 'sector' not in cotations.columns:
                cotations = tests.validator._prepare_data(cotations, is_cotations=True)
            # 1. Market Index Analysis
            if 'TUNINDEX' in df['lib_indice'].values:
                market_data = df[df['lib_indice'] == 'TUNINDEX']
                results['market_stationarity'] = validator.stationarity_test(market_data['indice_jour'])
            
            # 2. Sector Index Relationships
            sector_indices = ['TUNBANQ', 'TUNASS', 'TUNFIN']
            for idx in sector_indices:
                if idx in df['lib_indice'].values:
                    idx_data = df[df['lib_indice'] == idx]
                    results[f'{idx}_cointegration'] = validator.cointegration_test(
                        df[df['lib_indice'] == 'TUNINDEX']['indice_jour'],
                        idx_data['indice_jour']
                    )
            
            # 3. Granger Causality Tests
            financial_pairs = [('TUNBANQ', 'TUNASS'), ('TUNBANQ', 'TUNFIN'), ('TUNASS', 'TUNFIN')]
            for s1, s2 in financial_pairs:
                if s1 in df['lib_indice'].values and s2 in df['lib_indice'].values:
                    results[f'{s1}_to_{s2}_granger'] = validator.granger_test(
                        df[df['lib_indice'] == s1]['indice_jour'],
                        df[df['lib_indice'] == s2]['indice_jour']
                    )
        except Exception as e:
            results['advanced_analysis_error'] = str(e)
        return results    
    
    def _validate_cotations(self, df: pd.DataFrame) -> dict:
        """Runs all statistical tests on cotations data with sector-aware checks."""
        results = {}
        validator = tests.DiamondValidator()
        

        # 1. Basic Distribution Tests
        results['price_distribution'] = validator.normality_test(df['cloture'])
        results['volume_distribution'] = validator.normality_test(df['quantite_negociee'])

        # 2. Sector-Based Analysis
        if 'sector' in df.columns and df['sector'].nunique() > 1:  # Ensure multiple sectors exist
            sectors = df['sector'].unique()
            for sector in sectors:
                sector_data = df[df['sector'] == sector]
                if len(sector_data) >= validator.min_sector_obs:
                    results[f'{sector}_stationarity'] = validator.stationarity_test(sector_data['cloture'])
        else:
          self.logger.warning("Insufficient sectors for analysis")
        # 3. Technical Indicator Validation
        results.update(validator.validate_technical_indicators(df))
        return results  
    def _train_models(self, cotations: pd.DataFrame) -> dict:
        """Trains CNN-LSTM and Transformer models on top 5 stocks."""
        results = {}
        top_stocks = cotations['valeur'].value_counts().nlargest(5).index.tolist()

        for stock in top_stocks:
            stock_data = cotations[cotations['valeur'] == stock]
            prices = stock_data['cloture'].values
            X, y = self.model_builder._create_sequences(prices)
            X = X.reshape((X.shape[0], X.shape[1], 1))

            # Train CNN-LSTM
            cnn_lstm = self.model_builder.build_cnn_lstm((X.shape[1], 1))
            history = self.model_builder.train_model(cnn_lstm, X, y ,  epochs=20)
            results[f'{stock}_cnn_lstm'] = {
                'final_loss': history.history['loss'][-1],
                'parameters': cnn_lstm.count_params()
            }

            # Train Transformer
            transformer = self.model_builder.build_transformer((X.shape[1], 1))
            history = self.model_builder.train_model(transformer, X, y)
            results[f'{stock}_transformer'] = {
                'final_loss': history.history['loss'][-1],
                'parameters': transformer.count_params()
            }

        return results

    def _run_advanced_analysis(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> dict:
        """Executes GARCH, VAR, and correlation strategies on all relevant pairs."""
        results = {}
        validator = AdvancedMarketValidator()
        try:
            # Prepare data first (add sector mapping if missing)
            if 'sector' not in cotations.columns:
                cotations = tests.validator._prepare_data(cotations, is_cotations=True)
            # 1. GARCH Volatility for All Sectors
            sectors = cotations['sector'].unique()
            for sector in sectors:
                sector_returns = cotations[cotations['sector'] == sector]['cloture'].pct_change().dropna()
                if len(sector_returns) >= 100:  # Minimum for GARCH
                    results[f'{sector}_garch'] = validator.garch_volatility_analysis(sector_returns)

            # 2. Correlation Strategies for Non-Cointegrated Pairs
            indices_pivot = indices.pivot(index='seance', columns='lib_indice', values='indice_jour')
            market = indices_pivot.get('TUNINDEX')
            if market is not None:
                for idx in indices_pivot.columns:
                    if idx != 'TUNINDEX':
                        try:
                            results[f'{idx}_correlation'] = validator.rolling_correlation_strategy(
                                market, indices_pivot[idx], window=30, threshold=1.5
                            )
                        except Exception as e:
                         self.logger.error(f"Correlation strategy failed for {idx}: {str(e)}")
            # 3. VAR Model for Top 5 Indices
            top_indices = indices['lib_indice'].value_counts().nlargest(5).index.tolist()
            if len(top_indices) >= 2:
                results['var_model'] = validator.vector_autoregression(indices_pivot[top_indices].dropna())
        except Exception as e:
            self.logger.error(f"Advanced analysis failed: {str(e)}")
            results['advanced_analysis_error'] = str(e)
    
        return results  
    def run_pipeline(self):
        """Orchestrates the full Diamond Layer pipeline with comprehensive validation and modeling."""
        try:
            self._log_phase_start("INITIALIZATION")
            self.logger.info("STARTING DIAMOND LAYER PIPELINE", extra={'phase': 'SYSTEM'})

            # --- 1. Initialize Database ---
            self.initialize_database()  # Ensures tables exist
            self.logger.info("Database schema verified", extra={'phase': 'INITIALIZATION'})

            # --- 2. Load Data ---
            self._log_phase_start("DATA_LOADING")
            cotations, indices = self.load_from_golden()
            self.logger.info(
                f"Data loaded: {len(cotations)} cotations, {len(indices)} indices",
                extra={'phase': 'DATA_LOADING'}
            )

            # Debug: Log column names and sample counts
            self._log_data_stats(cotations, indices)

            # --- 3. Run Core Validations ---
            self._log_phase_start("CORE_VALIDATION")
            try:
                validation_results = {
                    'cotations': self._validate_cotations(cotations),
                    'indices': self._validate_indices(indices)
                }
                self.logger.info(
                    "Core validation completed",
                    extra={'phase': 'CORE_VALIDATION', 'tests_run': len(validation_results)}
                )
            except Exception as e:
                self.logger.error(f"Core validation failed: {str(e)}")
                validation_results = {'core_validation_error': str(e)}
            # --- 4. Advanced Analysis ---
            if self.enable_advanced_models:
                self._log_phase_start("ADVANCED_ANALYSIS")
                advanced_results = self._run_advanced_analysis(cotations, indices)
                validation_results.update(advanced_results)
                self.logger.info(
                    f"Advanced tests completed: {len(advanced_results)}",
                    extra={'phase': 'ADVANCED_ANALYSIS'}
                )

            # --- 5. Model Training ---
            if self.enable_advanced_models:
                self._log_phase_start("MODEL_TRAINING")
                model_results = self._train_models(cotations)
                validation_results['model_performance'] = model_results
                self.logger.info(
                    f"Models trained: {len(model_results)}",
                    extra={'phase': 'MODEL_TRAINING'}
                )

            # --- 6. Save Results ---
            self._log_phase_start("RESULT_SAVING")
            self.save_results(validation_results)
            self.logger.info(
                f"Saved {len(validation_results)} results",
                extra={'phase': 'RESULT_SAVING'}
            )

            # --- 7. Final Report ---
            print(tests.generate_validation_report(validation_results))
            self.logger.info(
                "PIPELINE COMPLETED SUCCESSFULLY",
                extra={'phase': 'COMPLETION'}
            )
            return {'status': 'success', 'results': validation_results}

        except Exception as e:
            self.logger.critical(
                f"PIPELINE FAILED: {str(e)}",
                extra={'phase': self.phase or 'UNKNOWN'},
                exc_info=True
            )
            return {'status': 'failed', 'error': str(e)}

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    pipeline = DiamondLayer()
    result = pipeline.run_pipeline()
    print(result)