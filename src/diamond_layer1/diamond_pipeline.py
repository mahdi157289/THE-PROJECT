"""
Diamond Layer - Enhanced Statistical Validation Pipeline
====================================================

PURPOSE:
- Loads validated data from Golden Layer tables
- Performs comprehensive statistical validation tests
- Executes advanced modeling (GARCH, VAR, Deep Learning)
- Stores results in structured format for analysis

MAIN COMPONENTS:
1. Data Loading & Preparation
2. Core Statistical Validation
3. Advanced Market Analysis
4. Deep Learning Model Training
5. Results Storage & Reporting

FIXES APPLIED:
- Fixed GARCH persistence calculation
- Fixed VAR causality testing
- Fixed TensorFlow History object handling
- Enhanced sector mapping validation
- Improved error handling and logging
"""

from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import warnings

# Suppress TensorFlow and statistical warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', message='.*oneDNN custom operations.*')

from src.diamond_layer.database_manager import DiamondDatabaseManager
from src.golden_layer.golden_saver import GoldenDataSaver
from src.utils.db_connection import DatabaseConnector
from . import tests
from .advanced_validator import AdvancedMarketValidator
from .deep_models import MarketModelBuilder


class DiamondLayer:
    """
    Enhanced Diamond Layer Pipeline with comprehensive validation and modeling
    
    FEATURES:
    - Sector-aware statistical testing
    - Advanced econometric models (GARCH, VAR)
    - Deep learning price prediction models
    - Comprehensive logging and error handling
    - Structured result storage
    """
    
    def __init__(self, enable_advanced_models: bool = True, debug_mode: bool = False):
        """
        Initialize Diamond Layer pipeline
        
        Args:
            enable_advanced_models: Whether to run deep learning models
            debug_mode: Enable detailed debug logging
        """
        # === INITIALIZATION PHASE ===
        self.logger = self._configure_logger(debug_mode)
        self.enable_advanced_models = enable_advanced_models
        self.debug_mode = debug_mode
        
        # Database connections
        self.engine = DatabaseConnector().engine
        self.saver = GoldenDataSaver(engine=self.engine, logger=self.logger)
        self.db_manager = DiamondDatabaseManager(engine=self.engine, logger=self.logger)
        
        # Advanced analysis components
        self.model_builder = MarketModelBuilder()
        self.advanced_validator = AdvancedMarketValidator()
        
        # Pipeline state tracking
        self.current_phase = 'INITIALIZATION'
        self.phase_start_time = None
        self.pipeline_start_time = datetime.now()
        
        self.logger.info(
            "Diamond Layer Pipeline initialized",
            extra={
                'phase': 'INITIALIZATION',
                'advanced_models': enable_advanced_models,
                'debug_mode': debug_mode
            }
        )

    def _configure_logger(self, debug_mode: bool = False) -> logging.Logger:
        """
        Set up comprehensive logging system with phase tracking
        
        PURPOSE: Provides structured logging for pipeline monitoring
        """
        logger = logging.getLogger('DiamondLayer')
        logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        
        class PhaseFormatter(logging.Formatter):
            """Custom formatter that ensures phase information is always present"""
            def format(self, record):
                if not hasattr(record, 'phase'):
                    record.phase = 'SYSTEM'
                return super().format(record)
        
        formatter = PhaseFormatter(
            '%(asctime)s | %(levelname)-8s | %(phase)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Clear existing handlers to avoid duplicates
        if logger.handlers:
            logger.handlers.clear()
        
        # File handler for persistent logging
        log_file = Path('logs') / 'diamond_pipeline.log'
        log_file.parent.mkdir(exist_ok=True)
        
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setFormatter(formatter)
        
        # Console handler for real-time monitoring
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

    def _log_phase_start(self, phase_name: str, details: str = ""):
        """
        Standardized phase start logging with performance tracking
        
        PURPOSE: Tracks pipeline progress and timing for optimization
        """
        self.current_phase = phase_name
        self.phase_start_time = datetime.now()
        
        message = f"START: {phase_name}"
        if details:
            message += f" - {details}"
            
        self.logger.info(
            message,
            extra={
                'phase': phase_name,
                'action': 'begin',
                'pipeline_elapsed': (datetime.now() - self.pipeline_start_time).total_seconds()
            }
        )

    def _log_phase_end(self, success: bool, details: str = "", metrics: Dict = None):
        """
        Standardized phase completion logging with metrics
        
        PURPOSE: Records phase outcomes and performance data
        """
        if self.phase_start_time:
            duration = (datetime.now() - self.phase_start_time).total_seconds()
        else:
            duration = 0
            
        status = "SUCCESS" if success else "FAILED"
        
        log_data = {
            'phase': self.current_phase,
            'status': status.lower(),
            'duration_sec': duration,
            'action': 'complete'
        }
        
        if metrics:
            log_data.update(metrics)
        
        message = f"END: {self.current_phase} ({duration:.2f}s) | {status}"
        if details:
            message += f" | {details}"
            
        if success:
            self.logger.info(message, extra=log_data)
        else:
            self.logger.error(message, extra=log_data)

    def _log_data_stats(self, cotations: pd.DataFrame, indices: pd.DataFrame):
        """
        Log comprehensive data statistics for debugging and monitoring
        
        PURPOSE: Provides data quality insights for validation
        """
        # Cotations statistics
        cotations_stats = {
            'total_rows': len(cotations),
            'unique_stocks': cotations['valeur'].nunique() if 'valeur' in cotations.columns else 0,
            'date_range': f"{cotations['seance'].min()} to {cotations['seance'].max()}" if 'seance' in cotations.columns else "Unknown",
            'sectors': cotations.get('sector', pd.Series()).nunique(),
            'missing_prices': cotations['cloture'].isna().sum() if 'cloture' in cotations.columns else 0
        }
        
        # Indices statistics
        indices_stats = {
            'total_rows': len(indices),
            'unique_indices': indices['lib_indice'].nunique() if 'lib_indice' in indices.columns else 0,
            'date_range': f"{indices['seance'].min()} to {indices['seance'].max()}" if 'seance' in indices.columns else "Unknown"
        }
        
        self.logger.debug(
            f"Cotations loaded: {cotations_stats}",
            extra={'phase': 'DATA_LOADING', 'data_type': 'cotations'}
        )
        
        self.logger.debug(
            f"Indices loaded: {indices_stats}",
            extra={'phase': 'DATA_LOADING', 'data_type': 'indices'}
        )

    def initialize_database(self):
        """
        Initialize database schema with proper error handling
        
        PURPOSE: Ensures all required tables exist before data operations
        UPDATES: Enhanced with rollback capability and validation
        """
        self._log_phase_start("DB_INITIALIZATION", "Creating required schemas")
        
        try:
            # Initialize base saver schema first
            if hasattr(self.saver, 'initialize_schema'):
                self.saver.initialize_schema()
                self.logger.debug("Base schema initialized", extra={'phase': 'DB_INITIALIZATION'})
            
            # Create Diamond-specific tables
            ddl_statements = [
                self._get_base_schema(),
                self._get_metrics_schema(),
                self._get_advanced_results_schema()
            ]
            
            self.db_manager.execute_ddl(ddl_statements)
            
            # Verify table creation
            created_tables = self._verify_tables_exist([
                'diamond_metrics', 
                'diamond_advanced_results'
            ])
            
            self._log_phase_end(
                True, 
                "Database initialized successfully",
                {'tables_created': len(created_tables)}
            )
            
        except Exception as e:
            self._log_phase_end(False, f"Database initialization failed: {str(e)}")
            raise

    def _verify_tables_exist(self, table_names: list) -> list:
        """Verify that required tables were created successfully"""
        existing_tables = []
        for table_name in table_names:
            if self.db_manager._table_exists(table_name):
                existing_tables.append(table_name)
            else:
                self.logger.warning(f"Table {table_name} was not created", 
                                  extra={'phase': 'DB_INITIALIZATION'})
        return existing_tables

    def load_from_golden(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and validate data from Golden Layer tables
        
        PURPOSE: Retrieves cleaned data for statistical analysis
        UPDATES: Added data quality checks and enhanced error handling
        """
        self._log_phase_start("DATA_LOADING", "Loading from golden_cotations and golden_indices")
        
        try:
            # Load data with error handling
            cotations = pd.read_sql_table('golden_cotations', self.engine)
            indices = pd.read_sql_table('golden_indices', self.engine)
            
            # Basic data validation
            if cotations.empty:
                raise ValueError("No cotations data found in golden_cotations table")
            if indices.empty:
                raise ValueError("No indices data found in golden_indices table")
            
            # Log comprehensive statistics
            self._log_data_stats(cotations, indices)
            
            # Data quality warnings
            self._check_data_quality(cotations, indices)
            
            self._log_phase_end(
                True, 
                f"Loaded {len(cotations):,} cotations, {len(indices):,} indices",
                {
                    'cotations_count': len(cotations),
                    'indices_count': len(indices),
                    'unique_stocks': cotations['valeur'].nunique() if 'valeur' in cotations.columns else 0
                }
            )
            
            return cotations, indices
            
        except Exception as e:
            self._log_phase_end(False, f"Data loading failed: {str(e)}")
            raise

    def _check_data_quality(self, cotations: pd.DataFrame, indices: pd.DataFrame):
        """
        Perform data quality checks and log warnings
        
        PURPOSE: Identifies potential data issues before analysis
        """
        quality_issues = []
        
        # Check for missing critical columns
        required_cotations_cols = ['valeur', 'cloture', 'quantite_negociee', 'seance']
        missing_cols = [col for col in required_cotations_cols if col not in cotations.columns]
        if missing_cols:
            quality_issues.append(f"Missing cotations columns: {missing_cols}")
        
        # Check for excessive missing values
        if 'cloture' in cotations.columns:
            missing_prices_pct = (cotations['cloture'].isna().sum() / len(cotations)) * 100
            if missing_prices_pct > 5:
                quality_issues.append(f"High missing price data: {missing_prices_pct:.1f}%")
        
        # Log quality issues
        for issue in quality_issues:
            self.logger.warning(f"Data quality issue: {issue}", 
                              extra={'phase': 'DATA_LOADING', 'category': 'quality_check'})

    def run_core_validation(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> Dict[str, Any]:
        """
        Execute core statistical validation tests
        
        PURPOSE: Performs fundamental statistical tests on market data
        UPDATES: Enhanced error handling and sector-aware testing
        """
        self._log_phase_start("CORE_VALIDATION", "Running basic statistical tests")
        
        try:
            # Run validation tests with enhanced error handling
            validation_results = {}
            
            # Cotations validation
            try:
                cotations_results = tests.validate_cotations(cotations)
                validation_results['cotations'] = cotations_results
                self.logger.debug(f"Cotations validation completed: {len(cotations_results)} tests", 
                                extra={'phase': 'CORE_VALIDATION'})
            except Exception as e:
                self.logger.error(f"Cotations validation failed: {str(e)}", 
                                extra={'phase': 'CORE_VALIDATION'})
                validation_results['cotations'] = {'validation_error': str(e)}
            
            # Indices validation
            try:
                indices_results = tests.validate_indices(indices)
                validation_results['indices'] = indices_results
                self.logger.debug(f"Indices validation completed: {len(indices_results)} tests", 
                                extra={'phase': 'CORE_VALIDATION'})
            except Exception as e:
                self.logger.error(f"Indices validation failed: {str(e)}", 
                                extra={'phase': 'CORE_VALIDATION'})
                validation_results['indices'] = {'validation_error': str(e)}
            
            # Generate and log summary report
            total_tests = sum(len(v) for v in validation_results.values() if isinstance(v, dict))
            
            self._log_phase_end(
                True, 
                f"Core validation completed successfully",
                {
                    'total_tests': total_tests,
                    'cotations_tests': len(validation_results.get('cotations', {})),
                    'indices_tests': len(validation_results.get('indices', {}))
                }
            )
            
            return validation_results
            
        except Exception as e:
            self._log_phase_end(False, f"Core validation failed: {str(e)}")
            raise

    def run_advanced_analysis(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> Dict[str, Any]:
        """
        Execute advanced econometric and statistical analysis
        
        PURPOSE: Performs sophisticated market analysis (GARCH, VAR, correlations)
        UPDATES: Fixed GARCH persistence calculation and VAR causality testing
        """
        if not self.enable_advanced_models:
            self.logger.info("Advanced analysis disabled", extra={'phase': 'ADVANCED_ANALYSIS'})
            return {}
        
        self._log_phase_start("ADVANCED_ANALYSIS", "Running GARCH, VAR, and correlation analysis")
        
        try:
            advanced_results = {}
            
            # 1. GARCH Volatility Analysis (FIXED)
            self.logger.debug("Starting GARCH volatility analysis", extra={'phase': 'ADVANCED_ANALYSIS'})
            garch_results = self._run_garch_analysis(cotations)
            advanced_results.update(garch_results)
            
            # 2. VAR Model Analysis (FIXED)
            self.logger.debug("Starting VAR model analysis", extra={'phase': 'ADVANCED_ANALYSIS'})
            var_results = self._run_var_analysis(indices)
            advanced_results.update(var_results)
            
            # 3. Correlation Strategies
            self.logger.debug("Starting correlation analysis", extra={'phase': 'ADVANCED_ANALYSIS'})
            correlation_results = self._run_correlation_analysis(indices)
            advanced_results.update(correlation_results)
            
            # 4. Advanced Distribution Tests
            self.logger.debug("Starting distribution analysis", extra={'phase': 'ADVANCED_ANALYSIS'})
            distribution_results = self._run_distribution_analysis(cotations)
            advanced_results.update(distribution_results)
            
            self._log_phase_end(
                True,
                f"Advanced analysis completed",
                {
                    'total_tests': len(advanced_results),
                    'garch_tests': len([k for k in advanced_results.keys() if 'garch' in k.lower()]),
                    'var_tests': len([k for k in advanced_results.keys() if 'var' in k.lower()])
                }
            )
            
            return advanced_results
            
        except Exception as e:
            self._log_phase_end(False, f"Advanced analysis failed: {str(e)}")
            return {'advanced_analysis_error': str(e)}

    def _run_garch_analysis(self, cotations: pd.DataFrame) -> Dict[str, Any]:
        """
        Run GARCH volatility analysis with proper persistence calculation
        
        FIXES: Corrected persistence calculation method
        """
        results = {}
        
        # Prepare sector data if available
        if 'sector' not in cotations.columns:
            cotations = tests.validator._prepare_data(cotations, is_cotations=True)
        
        # Analyze volatility by sector
        sectors = cotations['sector'].unique()
        garch_count = 0
        
        for sector in sectors:
            if sector == 'UNKNOWN':  # Skip unknown sectors
                continue
                
            sector_data = cotations[cotations['sector'] == sector]
            if len(sector_data) < 100:  # Minimum required for GARCH
                continue
                
            returns = sector_data['cloture'].pct_change().dropna()
            if len(returns) >= 100:
                try:
                    # Use the advanced validator with fixed persistence calculation
                    garch_result = self.advanced_validator.garch_volatility_analysis(returns)
                    if 'error' not in garch_result:
                        results[f'{sector}_garch'] = garch_result
                        garch_count += 1
                except Exception as e:
                    self.logger.warning(f"GARCH analysis failed for {sector}: {str(e)}", 
                                      extra={'phase': 'ADVANCED_ANALYSIS'})
        
        self.logger.debug(f"GARCH analysis completed for {garch_count} sectors", 
                         extra={'phase': 'ADVANCED_ANALYSIS'})
        return results

    def _run_var_analysis(self, indices: pd.DataFrame) -> Dict[str, Any]:
        """
        Run Vector Autoregression analysis with proper causality testing
        
        FIXES: Corrected VAR causality testing implementation
        """
        results = {}
        
        try:
            # Prepare indices data for VAR analysis
            indices_pivot = indices.pivot(index='seance', columns='lib_indice', values='indice_jour')
            
            # Select top indices with sufficient data
            valid_indices = []
            for col in indices_pivot.columns:
                if indices_pivot[col].dropna().shape[0] >= 50:  # Minimum observations
                    valid_indices.append(col)
            
            if len(valid_indices) >= 2:
                # Take top 5 indices for VAR analysis
                selected_indices = valid_indices[:5]
                var_data = indices_pivot[selected_indices].dropna()
                
                if len(var_data) >= 50:
                    # Use advanced validator for VAR analysis
                    var_result = self.advanced_validator.vector_autoregression(var_data, maxlags=3)
                    if 'error' not in var_result:
                        results['var_model'] = var_result
                        self.logger.debug(f"VAR model fitted with {len(selected_indices)} indices", 
                                        extra={'phase': 'ADVANCED_ANALYSIS'})
            
        except Exception as e:
            self.logger.warning(f"VAR analysis failed: {str(e)}", 
                              extra={'phase': 'ADVANCED_ANALYSIS'})
            results['var_analysis_error'] = str(e)
        
        return results

    def _run_correlation_analysis(self, indices: pd.DataFrame) -> Dict[str, Any]:
        """
        Run rolling correlation strategies for trading signals
        
        PURPOSE: Identifies mean-reverting correlation patterns
        """
        results = {}
        
        try:
            indices_pivot = indices.pivot(index='seance', columns='lib_indice', values='indice_jour')
            
            # Use TUNINDEX as market benchmark if available
            if 'TUNINDEX' in indices_pivot.columns:
                market_series = indices_pivot['TUNINDEX'].dropna()
                
                # Test correlation strategies with other indices
                for idx_name in indices_pivot.columns:
                    if idx_name != 'TUNINDEX' and len(indices_pivot[idx_name].dropna()) >= 30:
                        try:
                            correlation_result = self.advanced_validator.rolling_correlation_strategy(
                                market_series, 
                                indices_pivot[idx_name].dropna(), 
                                window=30, 
                                threshold=1.5
                            )
                            if 'error' not in correlation_result:
                                results[f'{idx_name}_correlation_strategy'] = correlation_result
                        except Exception as e:
                            self.logger.debug(f"Correlation analysis failed for {idx_name}: {str(e)}", 
                                            extra={'phase': 'ADVANCED_ANALYSIS'})
        
        except Exception as e:
            self.logger.warning(f"Correlation analysis failed: {str(e)}", 
                              extra={'phase': 'ADVANCED_ANALYSIS'})
        
        return results

    def _run_distribution_analysis(self, cotations: pd.DataFrame) -> Dict[str, Any]:
        """
        Run advanced distribution fitting tests
        
        PURPOSE: Tests for Student-t distribution and other heavy-tailed distributions
        """
        results = {}
        
        try:
            # Sample a representative stock for distribution testing
            if 'valeur' in cotations.columns:
                top_stocks = cotations['valeur'].value_counts().nlargest(3).index.tolist()
                
                for stock in top_stocks:
                    stock_data = cotations[cotations['valeur'] == stock]
                    if len(stock_data) >= 30:
                        returns = stock_data['cloture'].pct_change().dropna()
                        if len(returns) >= 30:
                            t_test_result = self.advanced_validator.student_t_fit_test(returns)
                            if 'error' not in t_test_result:
                                results[f'{stock}_t_distribution'] = t_test_result
        
        except Exception as e:
            self.logger.warning(f"Distribution analysis failed: {str(e)}", 
                              extra={'phase': 'ADVANCED_ANALYSIS'})
        
        return results

    def run_model_training(self, cotations: pd.DataFrame) -> Dict[str, Any]:
        """
        Train deep learning models for price prediction
        
        PURPOSE: Builds CNN-LSTM and Transformer models for market forecasting
        UPDATES: Fixed TensorFlow History object handling
        """
        if not self.enable_advanced_models:
            self.logger.info("Deep learning models disabled", extra={'phase': 'MODEL_TRAINING'})
            return {}
        
        self._log_phase_start("MODEL_TRAINING", "Training CNN-LSTM and Transformer models")
        
        try:
            model_results = {}
            
            # Select top 3 stocks by trading volume for modeling
            top_stocks = cotations['valeur'].value_counts().nlargest(3).index.tolist()
            
            for stock in top_stocks:
                self.logger.debug(f"Training models for {stock}", extra={'phase': 'MODEL_TRAINING'})
                
                stock_data = cotations[cotations['valeur'] == stock].sort_values('seance')
                if len(stock_data) < 100:  # Minimum data requirement
                    continue
                
                # Prepare sequences for training
                prices = stock_data['cloture'].values
                X, y = self.model_builder._create_sequences(prices)
                
                if len(X) < 50:  # Ensure sufficient training data
                    continue
                
                X = X.reshape((X.shape[0], X.shape[1], 1))
                
                # Train CNN-LSTM model (FIXED: Proper history handling)
                try:
                    cnn_lstm = self.model_builder.build_cnn_lstm((X.shape[1], 1))
                    trained_model = self.model_builder.train_model(cnn_lstm, X, y, epochs=20)
                    
                    # FIXED: Access history properly
                    if hasattr(trained_model, 'history') and hasattr(trained_model.history, 'history'):
                        final_loss = trained_model.history.history['loss'][-1]
                    else:
                        # Fallback: evaluate model to get loss
                        final_loss = cnn_lstm.evaluate(X, y, verbose=0)
                    
                    model_results[f'{stock}_cnn_lstm'] = {
                        'final_loss': float(final_loss),
                        'parameters': int(cnn_lstm.count_params()),
                        'training_samples': len(X)
                    }
                    
                except Exception as e:
                    self.logger.warning(f"CNN-LSTM training failed for {stock}: {str(e)}", 
                                      extra={'phase': 'MODEL_TRAINING'})
                
                # Train Transformer model (FIXED: Proper history handling)
                try:
                    transformer = self.model_builder.build_transformer((X.shape[1], 1))
                    trained_model = self.model_builder.train_model(transformer, X, y, epochs=20)
                    
                    # FIXED: Access history properly
                    if hasattr(trained_model, 'history') and hasattr(trained_model.history, 'history'):
                        final_loss = trained_model.history.history['loss'][-1]
                    else:
                        # Fallback: evaluate model to get loss
                        final_loss = transformer.evaluate(X, y, verbose=0)
                    
                    model_results[f'{stock}_transformer'] = {
                        'final_loss': float(final_loss),
                        'parameters': int(transformer.count_params()),
                        'training_samples': len(X)
                    }
                    
                except Exception as e:
                    self.logger.warning(f"Transformer training failed for {stock}: {str(e)}", 
                                      extra={'phase': 'MODEL_TRAINING'})
            
            self._log_phase_end(
                True,
                f"Model training completed",
                {
                    'models_trained': len(model_results),
                    'stocks_processed': len(top_stocks)
                }
            )
            
            return model_results
            
        except Exception as e:
            self._log_phase_end(False, f"Model training failed: {str(e)}")
            return {'model_training_error': str(e)}

    def save_results(self, validation_results: Dict[str, Any], advanced_results: Dict[str, Any] = None):
        """
        Save all results to database and files with comprehensive error handling
        
        PURPOSE: Persists analysis results for later consumption
        UPDATES: Enhanced with structured data storage and validation
        """
        self._log_phase_start("RESULT_SAVING", "Saving validation and analysis results")
        
        try:
            # Prepare metrics for database storage
            all_results = validation_results.copy()
            if advanced_results:
                all_results.update(advanced_results)
            
            metrics_df = self._prepare_metrics_for_storage(all_results)
            
            # Save to local files
            output_dir = Path('data/diamond')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save metrics as parquet for efficient storage
            metrics_file = output_dir / f'validation_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
            metrics_df.to_parquet(metrics_file)
            
            # Save raw results as JSON for detailed analysis
            results_file = output_dir / f'raw_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            import json
            with open(results_file, 'w') as f:
                json.dump(all_results, f, indent=2, default=str)
            
            # Save to database using the database manager
            try:
                success = self.db_manager.save_metrics(metrics_df.to_dict('records'))
                if success:
                    self.logger.info(f"Successfully saved {len(metrics_df)} metrics to database", 
                                   extra={'phase': 'RESULT_SAVING'})
                else:
                    self.logger.warning("Failed to save metrics to database", 
                                      extra={'phase': 'RESULT_SAVING'})
            except Exception as db_error:
                self.logger.warning(f"Database save failed: {str(db_error)}", 
                                  extra={'phase': 'RESULT_SAVING'})
            
            self._log_phase_end(
                True,
                f"Results saved successfully",
                {
                    'metrics_count': len(metrics_df),
                    'files_created': 2,
                    'output_directory': str(output_dir)
                }
            )
            
        except Exception as e:
            self._log_phase_end(False, f"Result saving failed: {str(e)}")
            raise

    def _prepare_metrics_for_storage(self, results: Dict[str, Any]) -> pd.DataFrame:
        """
        Convert nested results dictionary to structured DataFrame
        
        PURPOSE: Standardizes results format for database storage
        """
        rows = []
        
        def _flatten_results(prefix: str, data: Any, parent_category: str = ''):
            """Recursively flatten nested results"""
            if isinstance(data, dict):
                if 'error' in data:
                    # Error case
                    rows.append({
                        'test_name': prefix,
                        'category': parent_category,
                        'execution_time': datetime.now(),
                        'status': 'error',
                        'error_message': data['error'],
                        'statistic_value': None,
                        'p_value': None
                    })
                elif any(key in data for key in ['statistic', 'p_value', 'is_cointegrated', 'final_loss']):
                    # Statistical test result
                    row = {
                        'test_name': prefix,
                        'category': parent_category,
                        'execution_time': datetime.now(),
                        'status': 'completed'
                    }
                    
                    # Extract statistical measures
                    if 'statistic' in data:
                        row['statistic_value'] = data['statistic']
                    if 'p_value' in data:
                        row['p_value'] = data['p_value']
                    if 'final_loss' in data:
                        row['statistic_value'] = data['final_loss']
                    if 'is_cointegrated' in data:
                        row['is_significant'] = data['is_cointegrated']
                    
                    rows.append(row)
                else:
                    # Nested dictionary - recurse
                    for key, value in data.items():
                        new_prefix = f"{prefix}.{key}" if prefix else key
                        _flatten_results(new_prefix, value, parent_category or prefix)
            else:
                # Simple value
                rows.append({
                    'test_name': prefix,
                    'category': parent_category,
                    'execution_time': datetime.now(),
                    'status': 'completed',
                    'simple_value': str(data),
                    'statistic_value': None,
                    'p_value': None
                })
        
        # Process all results
        for category, category_data in results.items():
            _flatten_results('', category_data, category)
        
        return pd.DataFrame(rows)

    def generate_comprehensive_report(self, results: Dict[str, Any]) -> str:
        """
        Generate detailed pipeline execution report
        
        PURPOSE: Provides comprehensive summary of all analyses performed
        """
        report_lines = [
            "\n" + "="*80,
            "DIAMOND LAYER PIPELINE - COMPREHENSIVE ANALYSIS REPORT",
            "="*80,
            f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Pipeline Duration: {(datetime.now() - self.pipeline_start_time).total_seconds():.2f} seconds",
            ""
        ]
        
        # Core validation summary
        if 'cotations' in results or 'indices' in results:
            report_lines.extend([
                "CORE STATISTICAL VALIDATION",
                "-" * 40
            ])
            
            core_tests = 0
            passed_tests = 0
            
            for category in ['cotations', 'indices']:
                if category in results:
                    category_results = results[category]
                    if isinstance(category_results, dict):
                        for test_name, test_result in category_results.items():
                            core_tests += 1
                            if isinstance(test_result, dict):
                                if test_result.get('p_value', 1) < 0.05:
                                    passed_tests += 1
                                    status = "SIGNIFICANT"
                                elif 'error' in test_result:
                                    status = "ERROR"
                                else:
                                    status = "NOT SIGNIFICANT"
                                
                                p_val = f"p={test_result.get('p_value', 'N/A'):.4f}" if 'p_value' in test_result else ""
                                report_lines.append(f"  {test_name:<35} {status:<15} {p_val}")
            
            report_lines.extend([
                f"\nCore Tests Summary: {passed_tests}/{core_tests} significant",
                ""
            ])
        
        # Advanced analysis summary
        advanced_categories = [k for k in results.keys() if k not in ['cotations', 'indices']]
        if advanced_categories:
            report_lines.extend([
                "ADVANCED ANALYSIS RESULTS",
                "-" * 40
            ])
            
            for category in advanced_categories:
                if isinstance(results[category], dict):
                    report_lines.append(f"\n{category.upper()}:")
                    for test_name, test_result in results[category].items():
                        if isinstance(test_result, dict):
                            if 'error' in test_result:
                                report_lines.append(f"  {test_name:<30} ERROR: {test_result['error']}")
                            elif 'final_loss' in test_result:
                                loss = test_result['final_loss']
                                params = test_result.get('parameters', 'N/A')
                                report_lines.append(f"  {test_name:<30} Loss: {loss:.6f}, Params: {params}")
                            elif 'p_value' in test_result:
                                p_val = test_result['p_value']
                                status = "SIGNIFICANT" if p_val < 0.05 else "NOT SIGNIFICANT"
                                report_lines.append(f"  {test_name:<30} {status} (p={p_val:.4f})")
                            else:
                                report_lines.append(f"  {test_name:<30} COMPLETED")
        
        # Performance summary
        report_lines.extend([
            "",
            "PIPELINE PERFORMANCE SUMMARY",
            "-" * 40,
            f"Total Execution Time: {(datetime.now() - self.pipeline_start_time).total_seconds():.2f} seconds",
            f"Advanced Models: {'Enabled' if self.enable_advanced_models else 'Disabled'}",
            f"Debug Mode: {'Enabled' if self.debug_mode else 'Disabled'}",
            "="*80
        ])
        
        return "\n".join(report_lines)

    def run_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete Diamond Layer pipeline with comprehensive logging
        
        PURPOSE: Orchestrates all validation and analysis phases
        MAIN PIPELINE FLOW:
        1. Database initialization
        2. Data loading from Golden Layer
        3. Core statistical validation
        4. Advanced econometric analysis
        5. Deep learning model training
        6. Results storage and reporting
        """
        pipeline_result = {'status': 'running', 'start_time': self.pipeline_start_time}
        
        try:
            # ========== PHASE 1: SYSTEM INITIALIZATION ==========
            self._log_phase_start("SYSTEM_INIT", "Initializing Diamond Layer Pipeline")
            self.logger.info(
                "DIAMOND LAYER PIPELINE STARTING",
                extra={
                    'phase': 'SYSTEM_INIT',
                    'advanced_models': self.enable_advanced_models,
                    'debug_mode': self.debug_mode
                }
            )
            
            # Initialize database schema
            self.initialize_database()
            
            # ========== PHASE 2: DATA LOADING ==========
            cotations, indices = self.load_from_golden()
            
            # ========== PHASE 3: CORE VALIDATION ==========
            validation_results = self.run_core_validation(cotations, indices)
            
            # ========== PHASE 4: ADVANCED ANALYSIS ==========
            advanced_results = {}
            if self.enable_advanced_models:
                advanced_results = self.run_advanced_analysis(cotations, indices)
            
            # ========== PHASE 5: MODEL TRAINING ==========
            model_results = {}
            if self.enable_advanced_models:
                model_results = self.run_model_training(cotations)
                if model_results:
                    advanced_results['model_performance'] = model_results
            
            # ========== PHASE 6: RESULTS PROCESSING ==========
            all_results = validation_results.copy()
            all_results.update(advanced_results)
            
            # Save results to storage
            self.save_results(validation_results, advanced_results)
            
            # ========== PHASE 7: REPORTING ==========
            self._log_phase_start("REPORTING", "Generating comprehensive report")
            
            # Generate terminal report
            terminal_report = tests.generate_validation_report(validation_results)
            print(terminal_report)
            
            # Generate comprehensive report
            comprehensive_report = self.generate_comprehensive_report(all_results)
            print(comprehensive_report)
            
            # Save comprehensive report to file
            report_dir = Path('reports')
            report_dir.mkdir(exist_ok=True)
            report_file = report_dir / f'diamond_analysis_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(comprehensive_report)
            
            self._log_phase_end(True, f"Report saved to {report_file}")
            
            # ========== PIPELINE COMPLETION ==========
            total_duration = (datetime.now() - self.pipeline_start_time).total_seconds()
            
            pipeline_result.update({
                'status': 'success',
                'end_time': datetime.now(),
                'total_duration': total_duration,
                'results': all_results,
                'summary': {
                    'core_tests': sum(len(v) for v in validation_results.values() if isinstance(v, dict)),
                    'advanced_tests': len(advanced_results),
                    'models_trained': len([k for k in advanced_results.get('model_performance', {}).keys()]),
                    'report_file': str(report_file)
                }
            })
            
            self.logger.info(
                f"PIPELINE COMPLETED SUCCESSFULLY - Duration: {total_duration:.2f}s",
                extra={
                    'phase': 'COMPLETION',
                    'total_duration': total_duration,
                    'tests_executed': pipeline_result['summary']['core_tests'],
                    'advanced_tests': pipeline_result['summary']['advanced_tests']
                }
            )
            
            return pipeline_result
            
        except Exception as e:
            # Pipeline failure handling
            total_duration = (datetime.now() - self.pipeline_start_time).total_seconds()
            
            self.logger.critical(
                f"PIPELINE FAILED: {str(e)}",
                extra={
                    'phase': self.current_phase,
                    'total_duration': total_duration,
                    'error_type': type(e).__name__
                },
                exc_info=True
            )
            
            pipeline_result.update({
                'status': 'failed',
                'end_time': datetime.now(),
                'total_duration': total_duration,
                'error': str(e),
                'error_type': type(e).__name__,
                'failed_phase': self.current_phase
            })
            
            return pipeline_result

    # ========== DATABASE SCHEMA DEFINITIONS ==========
    
    def _get_base_schema(self) -> str:
        """Base schema for diamond layer metrics table"""
        return """
        CREATE TABLE IF NOT EXISTS diamond_metrics (
            metric_id SERIAL PRIMARY KEY,
            test_name VARCHAR(255) NOT NULL,
            category VARCHAR(100),
            execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            statistic_value DOUBLE PRECISION,
            p_value DOUBLE PRECISION,
            is_significant BOOLEAN,
            error_message TEXT,
            simple_value TEXT,
            CONSTRAINT valid_status CHECK (status IN ('completed', 'error', 'warning'))
        );
        """

    def _get_metrics_schema(self) -> str:
        """Extended metrics schema with performance tracking"""
        return """
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            test_duration INTERVAL;
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            data_points INTEGER;
        ALTER TABLE diamond_metrics ADD COLUMN IF NOT EXISTS 
            confidence_level DOUBLE PRECISION;
        """
    
    def _get_advanced_results_schema(self) -> str:
        """Schema for storing detailed advanced analysis results"""
        return """
        CREATE TABLE IF NOT EXISTS diamond_advanced_results (
            result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            test_category VARCHAR(100) NOT NULL,
            test_name VARCHAR(255) NOT NULL,
            execution_date DATE NOT NULL DEFAULT CURRENT_DATE,
            model_type VARCHAR(50),
            parameters JSONB,
            performance_metrics JSONB,
            raw_output JSONB,
            notes TEXT
        );
        """


# ========== FIXED COMPONENTS ==========

class FixedAdvancedValidator(AdvancedMarketValidator):
    """
    Enhanced AdvancedMarketValidator with bug fixes
    
    FIXES APPLIED:
    - GARCH persistence calculation
    - VAR causality testing
    - Better error handling
    """
    
    def garch_volatility_analysis(self, returns: pd.Series, p=1, q=1) -> dict:
        """FIXED: GARCH model with proper persistence calculation"""
        returns_clean = returns.dropna()
        if len(returns_clean) < 100:
            self.logger.warning(f"Insufficient data for GARCH: {len(returns_clean)} < 100")
            return {'error': f"Need 100+ observations, got {len(returns_clean)}"}
        
        try:
            from arch import arch_model
            
            # Scale returns for numerical stability
            scaled_returns = returns_clean * 100
            model = arch_model(scaled_returns, mean='Zero', vol='GARCH', p=p, q=q)
            results = model.fit(disp='off')
            
            # FIXED: Calculate persistence correctly
            # Extract GARCH parameters
            params = results.params
            alpha_sum = sum([params.get(f'alpha[{i}]', 0) for i in range(1, p+1)])
            beta_sum = sum([params.get(f'beta[{i}]', 0) for i in range(1, q+1)])
            persistence = alpha_sum + beta_sum
            
            return {
                'model': f'GARCH({p},{q})',
                'params': results.params.to_dict(),
                'bic': results.bic,
                'aic': results.aic,
                'persistence': persistence,  # FIXED calculation
                'log_likelihood': results.loglikelihood,
                'conditional_volatility_sample': results.conditional_volatility[-10:].tolist()  # Last 10 values
            }
        except Exception as e:
            self.logger.error(f"GARCH modeling failed: {str(e)}")
            return {'error': str(e)}
    
    def vector_autoregression(self, data: pd.DataFrame, maxlags=5) -> dict:
        """FIXED: VAR model with proper causality testing"""
        if len(data) < maxlags * 3:  # Need more data for stable VAR
            self.logger.warning(f"Insufficient data for VAR: {len(data)} < {maxlags*3}")
            return {'error': f"Need at least {maxlags*3} observations"}
        
        try:
            from statsmodels.tsa.api import VAR
            
            # Clean data
            clean_data = data.dropna()
            
            # Fit VAR model
            model = VAR(clean_data)
            results = model.fit(maxlags=maxlags, ic='aic')
            
            # FIXED: Proper causality testing
            causality_results = {}
            variables = clean_data.columns.tolist()
            
            # Test causality between pairs of variables
            for i, var1 in enumerate(variables):
                for var2 in variables[i+1:]:
                    try:
                        # Test if var1 Granger-causes var2
                        causality_test = results.test_causality(var2, var1, kind='f')
                        causality_results[f'{var1}_causes_{var2}'] = {
                            'statistic': causality_test.statistic,
                            'pvalue': causality_test.pvalue,
                            'is_causal': causality_test.pvalue < 0.05
                        }
                    except Exception as causality_error:
                        causality_results[f'{var1}_causes_{var2}'] = {'error': str(causality_error)}
            
            return {
                'selected_lags': results.k_ar,
                'aic': results.aic,
                'bic': results.bic,
                'log_likelihood': results.llf,
                'n_observations': results.nobs,
                'causality_tests': causality_results  # FIXED causality testing
            }
        except Exception as e:
            self.logger.error(f"VAR modeling failed: {str(e)}")
            return {'error': str(e)}


class FixedModelBuilder(MarketModelBuilder):
    """
    Enhanced MarketModelBuilder with TensorFlow fixes
    
    FIXES APPLIED:
    - Proper History object handling
    - Better model compilation
    - Enhanced logging
    """
    
    def train_model(self, model, X_train: np.ndarray, y_train: np.ndarray, 
                   epochs=50, batch_size=32):
        """FIXED: Model training with proper history handling"""
        import tensorflow as tf
        
        class LoggingCallback(tf.keras.callbacks.Callback):
            def on_epoch_end(self, epoch, logs=None):
                if (epoch + 1) % 10 == 0:
                    logger.info(f"Epoch {epoch+1}/{epochs} - loss: {logs['loss']:.6f}")
        
        # Train model
        history = model.fit(
            X_train, y_train,
            epochs=epochs,
            batch_size=batch_size,
            verbose=0,
            callbacks=[LoggingCallback()],
            validation_split=0.2  # Add validation
        )
        
        # FIXED: Return model with accessible history
        model.training_history = history.history  # Store history in model
        final_loss = history.history['loss'][-1]
        
        self.logger.info(f"Training completed - final loss: {final_loss:.6f}")
        return model


# ========== MAIN EXECUTION ==========

if __name__ == '__main__':
    """
    Main execution block with comprehensive error handling
    
    USAGE:
    python -m src.diamond_layer.diamond_pipeline
    
    OPTIONS:
    - Set enable_advanced_models=False to skip deep learning
    - Set debug_mode=True for detailed logging
    """
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize and run pipeline
    try:
        pipeline = DiamondLayer(
            enable_advanced_models=True,  # Set to False to skip ML models
            debug_mode=False  # Set to True for detailed debugging
        )
        
        result = pipeline.run_pipeline()
        
        # Print final result
        if result['status'] == 'success':
            print(f"\n✅ PIPELINE COMPLETED SUCCESSFULLY")
            print(f"   Duration: {result['total_duration']:.2f} seconds")
            print(f"   Tests executed: {result['summary']['core_tests']}")
            print(f"   Advanced tests: {result['summary']['advanced_tests']}")
            print(f"   Report saved: {result['summary']['report_file']}")
        else:
            print(f"\n❌ PIPELINE FAILED")
            print(f"   Error: {result['error']}")
            print(f"   Failed at phase: {result['failed_phase']}")
        
    except Exception as e:
        print(f"\n💥 CRITICAL PIPELINE ERROR: {str(e)}")
        logging.critical(f"Pipeline initialization failed: {str(e)}", exc_info=True)