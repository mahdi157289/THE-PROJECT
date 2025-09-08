import os
import pandas as pd
from pathlib import Path

def consolidate_csv_files():
    """Consolidate individual year CSV files into single files"""
    
    # Define paths
    scraper_dir = Path(__file__).parent / 'scrape_data_from_BVMT'
    output_dir = Path(__file__).parent.parent.parent / 'data' / 'input'
    
    # Create output directories
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'cotations').mkdir(exist_ok=True)
    (output_dir / 'indices').mkdir(exist_ok=True)
    
    print(f"🔍 Scraper directory: {scraper_dir}")
    print(f"📁 Output directory: {output_dir}")
    
    # Consolidate cotations
    print("\n📊 Consolidating cotations...")
    cotations_files = list((scraper_dir / 'csv_cotations').glob('*_histo_cotations.csv'))
    cotations_files.sort()
    
    if cotations_files:
        print(f"Found {len(cotations_files)} cotation files")
        cotations_dfs = []
        
        for file_path in cotations_files:
            print(f"  Reading {file_path.name}...")
            try:
                df = pd.read_csv(file_path, sep=';', low_memory=False)
                cotations_dfs.append(df)
                print(f"    ✓ {len(df)} rows")
            except Exception as e:
                print(f"    ❌ Error reading {file_path.name}: {e}")
        
        if cotations_dfs:
            consolidated_cotations = pd.concat(cotations_dfs, ignore_index=True)
            output_path = output_dir / 'cotations' / 'cotations.csv'
            consolidated_cotations.to_csv(output_path, index=False, sep=';')
            print(f"✅ Consolidated cotations: {len(consolidated_cotations)} rows -> {output_path}")
            print(f"   File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
    
    # Consolidate indices
    print("\n📈 Consolidating indices...")
    indices_files = list((scraper_dir / 'csv_indices').glob('*_histo_indices.csv'))
    indices_files.sort()
    
    if indices_files:
        print(f"Found {len(indices_files)} indices files")
        indices_dfs = []
        
        for file_path in indices_files:
            print(f"  Reading {file_path.name}...")
            try:
                df = pd.read_csv(file_path, sep=';', low_memory=False)
                indices_dfs.append(df)
                print(f"    ✓ {len(df)} rows")
            except Exception as e:
                print(f"    ❌ Error reading {file_path.name}: {e}")
        
        if indices_dfs:
            consolidated_indices = pd.concat(indices_dfs, ignore_index=True)
            output_path = output_dir / 'indices' / 'indices.csv'
            consolidated_indices.to_csv(output_path, index=False, sep=';')
            print(f"✅ Consolidated indices: {len(consolidated_indices)} rows -> {output_path}")
            print(f"   File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
    
    print("\n🎯 Consolidation complete!")
    print(f"📁 Check output in: {output_dir}")

if __name__ == "__main__":
    consolidate_csv_files()


