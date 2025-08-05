"""
Golden Layer Main Pipeline
Complete ETL process with phase tracking and error handling
"""

import io
import os
import sys
from venv import logger
import pandas as pd
import logging
from datetime import datetime
import dask.dataframe as dd

from src.golden_layer.golden_saver import GoldenDataSaver
from . import golden_features_creation, statistical_validation

from src.utils.db_connection import DatabaseConnector

class GoldenLayer:
    def __init__(self):
        self.logger = self._configure_logger()
        self.phase = None
        self.start_time = None

    def _configure_logger(self):
        """Set up comprehensive logging"""
        logger = logging.getLogger('GoldenLayer')
        logger.setLevel(logging.INFO)
        
        # Create formatter with safe formatting
        class SafeFormatter(logging.Formatter):
            def format(self, record):
                try:
                    return super().format(record)
                except KeyError as e:
                    # Fallback format if 'phase' is missing
                    if 'phase' not in record.__dict__:
                        record.__dict__['phase'] = 'NO_PHASE'
                    return super().format(record)
                except UnicodeEncodeError:
                    # Remove emojis if encoding fails
                    msg = record.msg.encode('ascii', 'ignore').decode('ascii')
                    record.msg = msg
                    return super().format(record)
        
        formatter = SafeFormatter(
            '%(asctime)s | %(levelname)-8s | %(phase)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler (detailed)
        fh = logging.FileHandler('golden_pipeline.log', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        
        # Console handler (summary)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        
        # Ensure we don't add duplicate handlers
        if not logger.handlers:
            logger.addHandler(fh)
            logger.addHandler(ch)
    
        return logger

    def _log_phase_start(self, phase_name: str):
        """Begin a new pipeline phase"""
        self.phase = phase_name
        self.start_time = datetime.now()
        self.logger.info(f"START", extra={'phase': phase_name})
        self.logger.debug(f"Parameters: {phase_name}", extra={'phase': phase_name})

    def _log_phase_end(self, success: bool, details: str = None):
        """Complete a pipeline phase"""
        duration = (datetime.now() - self.start_time).total_seconds()
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(
            f"COMPLETE ({duration:.2f}s) | {status} | {details or ''}",
            extra={'phase': self.phase}
        )
        self.phase = None

    def load_data(self, data_dir: str) -> tuple:
        """Load silver layer data with validation"""
        self._log_phase_start("DATA_LOADING")
        try:
            cotations = pd.read_parquet(os.path.join(data_dir, 'silver_cotations.parquet'))
            indices = pd.read_parquet(os.path.join(data_dir, 'silver_indices.parquet'))
            
            # Validate required columns exist
            required_cotation_cols = {'seance', 'valeur', 'cloture'}
            if not required_cotation_cols.issubset(cotations.columns):
                missing = required_cotation_cols - set(cotations.columns)
                raise ValueError(f"Missing columns in cotations: {missing}")
                
            required_index_cols = {'seance', 'lib_indice', 'indice_jour'}
            if not required_index_cols.issubset(indices.columns):
                missing = required_index_cols - set(indices.columns)
                raise ValueError(f"Missing columns in indices: {missing}")
                
            self._log_phase_end(True, f"Loaded {len(cotations)} cotations, {len(indices)} indices")
            return cotations, indices
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise

    def process(self, cotations: pd.DataFrame, indices: pd.DataFrame) -> tuple:
        """Full golden layer transformation"""
        # Data validation
        required_cotation_cols = {'seance', 'valeur', 'cloture', 'quantite_negociee', 'nb_transaction', 'capitaux'}
        if not required_cotation_cols.issubset(cotations.columns):
            missing = required_cotation_cols - set(cotations.columns)
            raise ValueError(f"Missing required columns in cotations: {missing}")

        required_index_cols = {'seance', 'lib_indice', 'indice_jour'}
        if not required_index_cols.issubset(indices.columns):
            missing = required_index_cols - set(indices.columns)
            raise ValueError(f"Missing required columns in indices: {missing}")
        # Debug: Print column info
        print("\n=== COTATIONS COLUMNS ===")
        print(cotations.columns.tolist())
        print("\n=== INDICES COLUMNS ===")
        print(indices.columns.tolist())    
        # 1. Sector Index Preparation
        self._log_phase_start("SECTOR_MAPPING")
        try:
            classifier = golden_features_creation.SectorClassifier()
            
            # Build sector indices, excluding pure indices and orphans
            sector_indices = {
                sector: indices[indices['lib_indice'] == sector].set_index('seance')['indice_jour']
                for sector in indices['lib_indice'].unique()
                if not classifier.is_pure_index(sector)
            }
            
            # Validate before proceeding
            if not classifier.validate_mappings(sector_indices.keys()):
                logger.warning("Some sectors will be excluded from processing")
            
            # Final filtered version
            sector_indices = {
                sector: data for sector, data in sector_indices.items()
                if sector not in classifier.orphan_sectors
            }
            
            if not sector_indices:
                raise ValueError("No valid sector indices remaining after filtering")
                
            self._log_phase_end(True, f"Mapped {len(sector_indices)} sectors "
                            f"(Excluded {len(classifier.orphan_sectors)} orphan sectors)")
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise    

        # 2. Feature Engineering
        self._log_phase_start("FEATURE_ENGINEERING")
        try:
            golden_cotations = golden_features_creation.enhance_cotations(cotations, sector_indices)
            golden_indices = golden_features_creation.enhance_indices(indices)
            self._log_phase_end(True, 
                f"Added {len(golden_cotations.columns)} features to cotations, "
                f"{len(golden_indices.columns)} to indices"
            )
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise

        # 3. Statistical Validation
        self._log_phase_start("STATISTICAL_VALIDATION")
        try:
            val_cotations = statistical_validation.validate_cotations(golden_cotations, sector_indices)
            val_indices = statistical_validation.validate_indices(golden_indices)
            self._log_phase_end(True, "Validation completed")
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise

        return golden_cotations, golden_indices
    def save_results(self, cotations: pd.DataFrame, indices: pd.DataFrame):
        """Store golden data using GoldenDataSaver"""
        self._log_phase_start("DATA_STORAGE")
        try:
            # Convert to dask DataFrames if needed
            cotations_dd = dd.from_pandas(cotations, npartitions=4)
            indices_dd = dd.from_pandas(indices, npartitions=2)
            
            # Initialize the new saver
            saver = GoldenDataSaver(
                engine=DatabaseConnector().engine,
                logger=self.logger
            )
            
            # Save results
            saver.save_results(
                cotations=cotations_dd,
                indices=indices_dd,
                output_dir='data/golden'
            )
            
            self._log_phase_end(True, 
                f"Saved {len(cotations)} cotations, {len(indices)} indices"
            )
        except Exception as e:
            self._log_phase_end(False, str(e))
            raise

    def run_pipeline(self):
        try:
            # Ensure phase is set
            self.phase = 'INITIALIZATION'
            self.logger.info("STARTING GOLDEN LAYER PIPELINE", extra={'phase': self.phase})
            
            # 1. Load Data
            self.phase = 'DATA_LOADING'
            cotations, indices = self.load_data('data/silver')
            
            # 2. Process
            self.phase = 'PROCESSING'
            golden_cotations, golden_indices = self.process(cotations, indices)
            print ( golden_indices , golden_indices )
            # 3. Save
            self.phase = 'DATA_STORAGE'
            self.save_results(golden_cotations, golden_indices)
            
            self.logger.info("PIPELINE COMPLETED SUCCESSFULLY", extra={'phase': 'COMPLETION'})
            return {
                'status': 'success',
                'cotations_count': len(golden_cotations),
                'indices_count': len(golden_indices)
                
            }
        except Exception as e:
            self.logger.critical(f"PIPELINE FAILED: {str(e)}", 
                            extra={'phase': self.phase or 'UNKNOWN'},
                            exc_info=True)
            return {
                'status': 'failed',
                'error': str(e),
                'phase': self.phase
            }

if __name__ == '__main__':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    logging.basicConfig(level=logging.DEBUG)
    pipeline = GoldenLayer()
    result = pipeline.run_pipeline()
    print(result)