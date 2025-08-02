"""
Golden Layer Pipeline - Ingests silver data, creates golden features, 
validates statistically, and stores in PostgreSQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine
from . import golden_features, statistical_validation
from .utils import load_silver_data, save_to_postgres

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GoldenLayer')

def main():
    """Main pipeline execution"""
    logger.info("Starting Golden Layer processing")
    
    # 1. Load silver data
    logger.info("Loading silver data from Parquet")
    silver_cotations = load_silver_data('silver_cotations.parquet')
    silver_indices = load_silver_data('silver_indices.parquet')
    
    # 2. Create golden features
    logger.info("Creating golden features")
    
    # Load market context data
    tnd_eur = pd.read_parquet('tnd_eur_rates.parquet')  # External data
    political_events = pd.read_parquet('political_events.parquet')  # External data
    
    # Process cotations
    cotations = golden_features.create_technical_indicators(silver_cotations)
    cotations = golden_features.create_fundamental_indicators(cotations, tnd_eur)
    cotations = golden_features.create_custom_features(cotations, political_events)
    
    # Process indices
    indices = golden_features.create_technical_indicators(silver_indices)
    indices = golden_features.create_custom_features(indices, political_events)
    
    # 3. Statistical validation
    logger.info("Running statistical validation")
    cotations_validation = statistical_validation.validate_golden_features(cotations)
    indices_validation = statistical_validation.validate_golden_features(indices)
    
    # Generate reports
    cotations_report = statistical_validation.generate_validation_report(cotations_validation)
    indices_report = statistical_validation.generate_validation_report(indices_validation)
    
    logger.info("Cotations Validation Results:\n%s", cotations_report)
    logger.info("Indices Validation Results:\n%s", indices_report)
    
    # 4. Save golden data
    logger.info("Saving golden data to PostgreSQL")
    save_to_postgres(cotations, 'golden_cotations')
    save_to_postgres(indices, 'golden_indices')
    
    # 5. Save to Parquet (optional)
    cotations.to_parquet('golden_cotations.parquet')
    indices.to_parquet('golden_indices.parquet')
    
    logger.info("Golden Layer processing completed successfully")

if __name__ == "__main__":
    main()