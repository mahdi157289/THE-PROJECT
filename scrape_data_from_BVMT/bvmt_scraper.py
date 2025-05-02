import os
import requests
import rarfile
import zipfile
import pandas as pd
import logging
import traceback
import time
import re
from io import StringIO
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bvmt_scraper.log", mode='w'),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://www.bvmt.com.tn/sites/default/files/historiques/indices/histo_indice_{year}.{ext}"
ALTERNATE_URL = "https://www.bvmt.com.tn/backoffice/historiques/indices_archive.php?annee={year}"

def verify_url_exists(url):
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException as e:
        logging.debug(f"URL verification failed: {e}")
        return False

def clean_numeric_columns(df):
    """Convert numeric columns from European format to float with enhanced handling"""
    # First clean up column names by stripping whitespace
    df.columns = df.columns.str.strip()
    
    # Handle different column name variations across years
    col_mapping = {
        'SEANCE': ['Date', 'SEANCE', 'Seance'],
        'CODE_INDICE': ['CODE_INDICE', 'Code', 'Index Code'],
        'LIB_INDICE': ['LIB_INDICE', 'Libellé', 'Index Name'],
        'INDICE_JOUR': ['INDICE_JOUR', 'Clôture', 'Closing'],
        'INDICE_VEILLE': ['INDICE_VEILLE', 'Clôture veille', 'Previous Close', 'INDICE_VEILLE'],
        'VARIATION_VEILLE': ['VARIATION_VEILLE', 'Var. %', 'Variation', 'Change %'],
        'INDICE_PLUS_HAUT': ['INDICE_PLUS_HAUT', 'Plus haut', 'High'],
        'INDICE_PLUS_BAS': ['INDICE_PLUS_BAS', 'Plus bas', 'Low'],
        'INDICE_OUV': ['INDICE_OUV', 'Ouverture', 'Open']
    }
    
    # Standardize column names
    new_columns = {}
    for standard_name, variants in col_mapping.items():
        for variant in variants:
            if variant in df.columns:
                new_columns[standard_name] = df[variant]
                break
    
    # If we found any matching columns, reconstruct the DataFrame
    if new_columns:
        df = pd.DataFrame(new_columns)
    
    numeric_cols = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                   'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
    
    for col in numeric_cols:
        if col in df.columns:
            # Remove any non-numeric characters except comma, dot and minus
            df[col] = df[col].astype(str).str.replace(r'[^\d,.-]', '', regex=True)
            
            # Handle cases where comma is used as decimal separator
            df[col] = df[col].str.replace(',', '.', regex=False)
            
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Handle date formats
    if 'SEANCE' in df.columns:
        # Try multiple date formats
        date_formats = ['%d/%m/%Y', '%d%m%Y', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']
        
        for date_format in date_formats:
            try:
                df['SEANCE'] = pd.to_datetime(
                    df['SEANCE'].astype(str).str.strip(),
                    format=date_format,
                    errors='raise'
                )
                break
            except ValueError:
                continue
        else:
            # If none of the formats worked, use coerce
            df['SEANCE'] = pd.to_datetime(df['SEANCE'], errors='coerce')
    
    return df

def parse_mixed_content(content, year=None):
    """Parse files that contain mixed formats (like 2021 and 2022 data together)"""
    sections = []
    current_section = []
    
    # Split content into sections based on empty lines
    for line in content.split('\n'):
        if line.strip() == '':
            if current_section:
                sections.append('\n'.join(current_section))
                current_section = []
        else:
            current_section.append(line)
    
    if current_section:
        sections.append('\n'.join(current_section))
    
    dfs = []
    for section in sections:
        # Check if section contains semicolon-delimited data (2022 format)
        if any(';' in line for line in section.split('\n')):
            try:
                # Clean lines by removing extra whitespace around semicolons
                cleaned_lines = []
                for line in section.split('\n'):
                    if ';' in line:
                        # Normalize the line by removing spaces around semicolons
                        cleaned_line = re.sub(r'\s*;\s*', ';', line.strip())
                        cleaned_lines.append(cleaned_line)
                
                if cleaned_lines:
                    df = pd.read_csv(StringIO('\n'.join(cleaned_lines)), sep=';', header=None)
                    if not df.empty:
                        # Assign column names for 2022 format
                        if df.shape[1] >= 9:
                            df.columns = [
                                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                                'INDICE_PLUS_BAS', 'INDICE_OUV'
                            ][:df.shape[1]]
                            dfs.append(df)
            except Exception as e:
                logging.debug(f"Failed to parse semicolon section: {str(e)}")
        
        # Check if section contains comma-delimited data (2021 format)
        elif any(',' in line for line in section.split('\n')):
            try:
                df = pd.read_csv(StringIO(section), header=None)
                if not df.empty:
                    # Assign column names for 2021 format
                    if df.shape[1] >= 9:
                        df.columns = [
                            'SEANCE', 'INDICE_JOUR', 'CODE_INDICE', 'INDICE_VEILLE',
                            'VARIATION_VEILLE', 'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS',
                            'INDICE_OUV', 'YEAR'
                        ][:df.shape[1]]
                        dfs.append(df)
            except Exception as e:
                logging.debug(f"Failed to parse comma section: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return clean_numeric_columns(combined_df)
    
    return pd.DataFrame()

def parse_fixed_width(content, year=None):
    """Parse the fixed-width BVMT file format with special handling for different years"""
    lines = [line for line in content.split('\n') if line.strip()]
    
    # First check if this is a mixed content file
    mixed_df = parse_mixed_content(content, year)
    if not mixed_df.empty:
        return mixed_df
    
    # Special handling for 2015 format
    if year == 2015:
        header_line = next((line for line in lines if 'SEANCE' in line and 'CODE' in line), None)
        if header_line:
            col_positions = [
                (0, 8),      # SEANCE
                (8, 30),     # CODE_INDICE
                (30, 110),   # LIB_INDICE
                (110, 122),  # INDICE_JOUR
                (122, 134),  # INDICE_VEILLE
                (134, 147),  # VARIATION_VEILLE
                (147, 160),  # INDICE_PLUS_HAUT
                (160, 173),  # INDICE_PLUS_BAS
                (173, None)  # INDICE_OUV
            ]
            
            header_idx = lines.index(header_line)
            data = []
            for line in lines[header_idx+2:]:
                if not line.strip() or line.strip().replace('-', '').strip() == '':
                    continue
                
                row = []
                for start, end in col_positions:
                    value = line[start:end].strip() if end else line[start:].strip()
                    row.append(value)
                
                if len(row) == 9:
                    data.append(row)
            
            columns = [
                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                'INDICE_PLUS_BAS', 'INDICE_OUV'
            ]
            return clean_numeric_columns(pd.DataFrame(data, columns=columns))
    
    # Standard parsing for other years
    header_idx = next((i for i, line in enumerate(lines) 
                     if 'SEANCE' in line and 'CODE_INDICE' in line), 0)
    
    if header_idx < len(lines):
        header = lines[header_idx]
        col_positions = [
            (header.index('SEANCE'), header.index('CODE_INDICE')),
            (header.index('CODE_INDICE'), header.index('LIB_INDICE')),
            (header.index('LIB_INDICE'), header.index('INDICE_JOUR')),
            (header.index('INDICE_JOUR'), header.index('INDICE_VEILLE')),
            (header.index('INDICE_VEILLE'), header.index('VARIATION_VEILLE')),
            (header.index('VARIATION_VEILLE'), header.index('INDICE_PLUS_HAUT')),
            (header.index('INDICE_PLUS_HAUT'), header.index('INDICE_PLUS_BAS')),
            (header.index('INDICE_PLUS_BAS'), header.index('INDICE_OUV')),
            (header.index('INDICE_OUV'), None)
        ]
        
        data = []
        for line in lines[header_idx+2:]:
            line = line.rstrip()
            if not line or line.replace('-', '').strip() == '':
                continue
                
            row = []
            for start, end in col_positions:
                value = line[start:end].strip() if end else line[start:].strip()
                row.append(value)
            
            if len(row) == 9:
                data.append(row)
        
        columns = [
            'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
            'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
            'INDICE_PLUS_BAS', 'INDICE_OUV'
        ]
        return clean_numeric_columns(pd.DataFrame(data, columns=columns))
    
    return pd.DataFrame()

def detect_and_read_file(file_path, year=None):
    """Read and parse BVMT index files with year-specific handling"""
    logging.info(f"Processing file: {file_path}")
    
    # First try parsing as mixed content (for files with both 2021 and 2022 data)
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        mixed_df = parse_mixed_content(content, year)
        if not mixed_df.empty:
            logging.info("Successfully parsed mixed content file")
            return mixed_df
    except Exception as e:
        logging.debug(f"Error parsing as mixed content: {str(e)}")
    
    # Try reading as CSV with semicolon separator (for 2022+)
    if year and year >= 2022:
        try:
            df = pd.read_csv(file_path, sep=';', header=None, skipinitialspace=True)
            if not df.empty:
                df.columns = [
                    'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                    'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                    'INDICE_PLUS_BAS', 'INDICE_OUV'
                ][:df.shape[1]]
                df = clean_numeric_columns(df)
                logging.info("Successfully read semicolon-separated file")
                return df
        except Exception as e:
            logging.debug(f"Error reading as semicolon CSV: {str(e)}")
    
    # Try reading as regular CSV
    if file_path.lower().endswith('.csv'):
        try:
            df = pd.read_csv(file_path)
            if not df.empty:
                df = clean_numeric_columns(df)
                logging.info("Successfully read CSV file")
                return df
        except Exception as e:
            logging.debug(f"Error reading as CSV: {str(e)}")
    
    # Try fixed-width parsing with different encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
            
            df = parse_fixed_width(content, year)
            
            if not df.empty:
                df = clean_numeric_columns(df)
                logging.info(f"Successfully read with encoding: {encoding}")
                return df
            
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.debug(f"Error with encoding {encoding}: {str(e)}")
            continue
    
    logging.error(f"Could not read file {file_path} with any method")
    return pd.DataFrame()

def download_and_extract(year, ext):
    """Download and extract archive for a specific year with filename correction"""
    url = BASE_URL.format(year=year, ext=ext)
    
    if not verify_url_exists(url):
        logging.warning(f"URL does not exist: {url}")
        return []
    
    os.makedirs("raw", exist_ok=True)
    os.makedirs("extracted", exist_ok=True)
    
    filename = f"histo_indice_{year}.{ext}"
    raw_path = os.path.join("raw", filename)
    
    try:
        logging.info(f"Downloading {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        extracted_files = []
        if ext == 'rar':
            try:
                with rarfile.RarFile(raw_path) as rf:
                    rf.extractall(path="extracted")
                    extracted_files = [os.path.join("extracted", f) for f in rf.namelist()]
            except rarfile.RarCannotExec:
                logging.error("RAR extraction failed - ensure unrar is installed")
                return []
        elif ext == 'zip':
            with zipfile.ZipFile(raw_path) as zf:
                zf.extractall(path="extracted")
                extracted_files = [os.path.join("extracted", f) for f in zf.namelist()]
        
        # Fix for 2015 filename issue
        corrected_files = []
        for f in extracted_files:
            if '20015' in f:
                corrected = f.replace('20015', '2015')
                os.rename(f, corrected)
                corrected_files.append(corrected)
                logging.info(f"Corrected filename from {f} to {corrected}")
            else:
                corrected_files.append(f)
        
        valid_extensions = ('.txt', '.csv')
        file_list = [f for f in corrected_files if f.lower().endswith(valid_extensions)]
        
        if year == 2015 and file_list:
            logging.info(f"2015 file content preview:")
            with open(file_list[0], 'r', encoding='latin1') as f:
                for _ in range(5):
                    line = next(f, '').strip()
                    logging.info(line)
        
        logging.info(f"Extracted files: {file_list}")
        return file_list
    
    except Exception as e:
        logging.error(f"Error processing {year} ({ext}): {traceback.format_exc()}")
        return []

def try_alternate_source(year):
    """Try alternative data source for missing years"""
    url = ALTERNATE_URL.format(year=year)
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            dfs = pd.read_html(StringIO(response.text))
            if dfs:
                df = dfs[0]
                df.columns = df.iloc[0]
                df = df[1:]
                df = clean_numeric_columns(df)
                return df
    except Exception as e:
        logging.warning(f"Failed to get alternate data for {year}: {str(e)}")
    return pd.DataFrame()

def clean_consolidated_data(df):
    """Clean the DataFrame by removing problematic rows and fixing data types"""
    # Remove rows that contain column headers in data fields
    column_headers = ['CODE_INDICE', 'LIB_INDICE', 'SEANCE', 'INDICE_JOUR', 
                     'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                     'INDICE_PLUS_BAS', 'INDICE_OUV']
    
    # Create a mask to identify rows that contain any column header in their data
    mask = df.apply(lambda row: any(str(header).lower() in str(cell).lower() 
                   for cell in row for header in column_headers), axis=1)
    
    # Keep only rows that don't match the mask
    df = df[~mask]
    
    # Clean numeric columns again to ensure consistency
    numeric_cols = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                   'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean date column
    if 'SEANCE' in df.columns:
        df['SEANCE'] = pd.to_datetime(df['SEANCE'], errors='coerce')
        df = df.dropna(subset=['SEANCE'])
    
    # Clean CODE_INDICE by stripping whitespace
    if 'CODE_INDICE' in df.columns:
        df['CODE_INDICE'] = df['CODE_INDICE'].astype(str).str.strip()
    
    # Remove any completely empty rows
    df = df.dropna(how='all')
    
    return df

def process_year(year):
    """Process data for a specific year with enhanced error handling"""
    os.makedirs("csv", exist_ok=True)
    csv_filename = f"{year}_histo_indice.csv"
    csv_path = os.path.join("csv", csv_filename)
    
    for ext in ['rar', 'zip']:
        extracted_files = download_and_extract(year, ext)
        
        if not extracted_files:
            logging.warning(f"No files found for {year} ({ext})")
            continue
        
        for source_file in extracted_files:
            try:
                df = detect_and_read_file(source_file, year)
                
                if not df.empty:
                    # Apply cleaning before saving
                    df = clean_consolidated_data(df)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    logging.info(f"Successfully saved {csv_path}")
                    return True
                else:
                    logging.warning(f"Empty DataFrame for {source_file}")
            except Exception as e:
                logging.error(f"Error processing {source_file}: {traceback.format_exc()}")
    
    logging.info(f"Trying alternate source for year {year}")
    df = try_alternate_source(year)
    if not df.empty:
        df = clean_consolidated_data(df)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logging.info(f"Saved from alternate source: {csv_path}")
        return True
    
    logging.warning(f"Could not process data for year {year}")
    return False

def generate_consolidated_csv(years):
    """Combine all yearly data into one consolidated CSV with cleaning"""
    dfs = []
    for year in years:
        csv_path = os.path.join("csv", f"{year}_histo_indice.csv")
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                
                # Clean the DataFrame before adding
                df = clean_consolidated_data(df)
                
                df['YEAR'] = year
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error reading {csv_path}: {str(e)}")
    
    if dfs:
        consolidated = pd.concat(dfs, ignore_index=True)
        
        # Additional cleaning for the consolidated DataFrame
        consolidated = clean_consolidated_data(consolidated)
        
        # Ensure consistent column ordering
        desired_columns = [
            'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
            'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
            'INDICE_PLUS_BAS', 'INDICE_OUV', 'YEAR'
        ]
        
        # Only keep columns that exist in the DataFrame
        available_columns = [col for col in desired_columns if col in consolidated.columns]
        consolidated = consolidated[available_columns]
        
        consolidated_path = os.path.join("..", "data", "input", "indices.csv")
        consolidated.to_csv(consolidated_path, index=False)
        
        file_size = os.path.getsize(consolidated_path) / (1024 * 1024)
        logging.info(f"Created consolidated file: {consolidated_path}")
        logging.info(f"Consolidated file size: {file_size:.2f} MB")
        
        return file_size
    return 0

def print_summary(start_time, successful_years, failed_years, file_size):
    """Print a nice summary of the execution"""
    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)
    
    print("\n" + "="*60)
    print("SCRIPT EXECUTION SUMMARY FOR INDICES FROM 2008 --> 2024".center(60))
    print("="*60)
    print(f"{'Total processing time:':<30} {int(minutes)} minutes {seconds:.2f} seconds")
    print(f"{'Successful years:':<30} {len(successful_years)}")
    print(f"{'Failed years:':<30} {len(failed_years)}")
    print(f"{'Consolidated file size:':<30} {file_size:.2f} MB")
    print("\nSuccessful years:", successful_years)
    print("Failed years:", failed_years)
    print("="*60 + "\n")

def main():
    """Main processing function"""
    start_time = time.time()
    successful_years = []
    failed_years = []
    
    start_year = 2008
    end_year = 2024
    for year in range(start_year, end_year + 1):
        logging.info(f"\n{'='*40}\nProcessing year: {year}\n{'='*40}")
        if process_year(year):
            successful_years.append(year)
        else:
            failed_years.append(year)
    
    file_size = generate_consolidated_csv(successful_years)
    print_summary(start_time, successful_years, failed_years, file_size)
    return successful_years, failed_years

if __name__ == "__main__":
    successful_years, failed_years = main()