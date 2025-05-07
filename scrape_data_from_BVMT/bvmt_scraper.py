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

BASE_URL = {
    "indices": "https://www.bvmt.com.tn/sites/default/files/historiques/indices/histo_indice_{year}.{ext}",
    "cotations": "https://www.bvmt.com.tn/sites/default/files/historiques/cotations/histo_cotation_{year}.{ext}"
}
ALTERNATE_URL = {
    "indices": "https://www.bvmt.com.tn/backoffice/historiques/indices_archive.php?annee={year}",
    "cotations": "https://www.bvmt.com.tn/backoffice/historiques/cotations_archive.php?annee={year}"
}

def verify_url_exists(url):
    try:
        response = requests.head(url, timeout=10)
        return response.status_code == 200
    except requests.RequestException as e:
        logging.debug(f"URL verification failed: {e}")
        return False

def clean_numeric_columns(df, data_type="indices"):
    """Convert numeric columns from European format to float with enhanced handling"""
    # First clean up column names by stripping whitespace
    df.columns = df.columns.str.strip()
    
    if data_type == "indices":
        # Handle different column name variations across years for indices
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
    else:
        # Cotation column mappings - based on the provided examples
        col_mapping = {
            'SEANCE': ['SEANCE', 'Date'],
            'CODE': ['CODE', 'CODE_VAL', 'CODE VALEUR'],
            'VALEUR': ['VALEUR', 'LIB_VAL'],
            'OUVERTURE': ['OUVERTURE', 'OUVERTURE'],
            'CLOTURE': ['CLOTURE', 'DERNIER_COURS'],
            'PLUS_BAS': ['PLUS_BAS', 'PLUS_BAS'],
            'PLUS_HAUT': ['PLUS_HAUT', 'PLUS_HAUT'],
            'QUANTITE_NEGOCIEE': ['QUANTITE_NEGOCIEE', 'QUANTITE NEGOCIEE'],
            'NB_TRANSACTION': ['NB_TRANSACTION', 'NB_TRAN', 'NOMBRE_TRANSACTION'],
            'CAPITAUX': ['CAPITAUX', 'CAPITAUX I']
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
        
        numeric_cols = ['OUVERTURE', 'CLOTURE', 'PLUS_BAS', 'PLUS_HAUT', 
                        'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
    
    for col in numeric_cols:
        if col in df.columns:
            # Remove any non-numeric characters except comma, dot and minus
            df[col] = df[col].astype(str).str.replace(r'[^\d,.-]', '', regex=True)
            
            # Handle cases where comma is used as decimal separator
            df[col] = df[col].str.replace(',', '.', regex=False)
            
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Handle date formats - with special handling for time components
    if 'SEANCE' in df.columns:
        # First remove time components if they exist (e.g., "11:00:00")
        df['SEANCE'] = df['SEANCE'].astype(str).str.replace(r'\s+\d{1,2}:\d{2}:\d{2}', '', regex=True).str.strip()
        
        # Try multiple date formats
        date_formats = ['%d/%m/%Y', '%d%m%Y', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%y']
        
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

def parse_mixed_content(content, year=None, data_type="indices"):
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
                        if data_type == "indices":
                            # Assign column names for 2022 indices format
                            if df.shape[1] >= 9:
                                df.columns = [
                                    'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                                    'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                                    'INDICE_PLUS_BAS', 'INDICE_OUV'
                                ][:df.shape[1]]
                        else:
                            # Assign column names for cotations format
                            if df.shape[1] >= 7:
                                df.columns = [
                                    'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                                    'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                                    'NB_TRANSACTION', 'CAPITAUX'
                                ][:df.shape[1]]
                        dfs.append(df)
            except Exception as e:
                logging.debug(f"Failed to parse semicolon section: {str(e)}")
        
        # Check if section contains comma-delimited data (2021 format)
        elif any(',' in line for line in section.split('\n')):
            try:
                df = pd.read_csv(StringIO(section), header=None)
                if not df.empty:
                    if data_type == "indices":
                        # Assign column names for 2021 indices format
                        if df.shape[1] >= 9:
                            df.columns = [
                                'SEANCE', 'INDICE_JOUR', 'CODE_INDICE', 'INDICE_VEILLE',
                                'VARIATION_VEILLE', 'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS',
                                'INDICE_OUV', 'YEAR'
                            ][:df.shape[1]]
                    else:
                        # Assign column names for cotations format
                        if df.shape[1] >= 7:
                            df.columns = [
                                'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE',
                                'NB_TRANSACTION', 'CAPITAUX'
                            ][:df.shape[1]]
                    dfs.append(df)
            except Exception as e:
                logging.debug(f"Failed to parse comma section: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return clean_numeric_columns(combined_df, data_type)
    
    return pd.DataFrame()

def parse_fixed_width(content, year=None, data_type="indices"):
    """Parse the fixed-width file format with special handling for different years"""
    lines = [line for line in content.split('\n') if line.strip()]
    
    # First check if this is a mixed content file
    mixed_df = parse_mixed_content(content, year, data_type)
    if not mixed_df.empty:
        return mixed_df
    
    # Handle cotation files
    if data_type == "cotations":
        # Try to detect header lines for cotations
        header_line = next((line for line in lines if 'SEANCE' in line and ('CODE' in line or 'VALEUR' in line)), None)
        
        if not header_line:
            # If can't find header with SEANCE and CODE, try broader match
            header_line = next((line for line in lines if any(term in line for term in 
                               ['SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE'])), None)
        
        if header_line:
            # Detect header columns
            columns = []
            column_positions = []
            
            # Search for these standard column names in the header
            search_cols = ['SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE', 'PLUS_BAS', 
                          'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
            
            # Try to find the positions of each column in the header
            last_pos = 0
            for col in search_cols:
                pos = header_line.find(col, last_pos)
                if pos >= 0:
                    columns.append(col)
                    column_positions.append(pos)
                    last_pos = pos + len(col)
            
            # Add the end position
            column_positions.append(len(header_line))
            
            if len(columns) >= 3:  # Need at least a few columns to be valid
                # Create position pairs for slicing
                col_positions = [(column_positions[i], column_positions[i+1]) 
                                for i in range(len(column_positions)-1)]
                
                # Find header index in lines
                header_idx = lines.index(header_line)
                
                # Find first data row
                data_start = header_idx + 1
                while data_start < len(lines) and ('-' * 5) in lines[data_start]:
                    data_start += 1
                
                data = []
                for line in lines[data_start:]:
                    if not line.strip() or line.strip().replace('-', '').strip() == '':
                        continue
                    
                    row = []
                    try:
                        for i, (start, end) in enumerate(col_positions):
                            # Handle case where line might be shorter than expected
                            if start >= len(line):
                                value = ""
                            elif end > len(line):
                                value = line[start:].strip()
                            else:
                                value = line[start:end].strip()
                            row.append(value)
                    except Exception as e:
                        logging.debug(f"Error parsing line: {line}, error: {str(e)}")
                        continue
                    
                    if row and len(row) == len(columns):
                        data.append(row)
                
                if data:
                    df = pd.DataFrame(data, columns=columns)
                    return clean_numeric_columns(df, data_type)
    
    # Special handling for 2015 format (or similar fixed-width formats)
    header_line = next((line for line in lines if 'SEANCE' in line), None)
    if header_line:
        # Try to identify columns and their positions
        col_names = []
        col_positions = []
        
        if data_type == "indices":
            search_cols = [
                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                'INDICE_PLUS_BAS', 'INDICE_OUV'
            ]
        else:
            search_cols = [
                'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE', 
                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                'NB_TRANSACTION', 'CAPITAUX'
            ]
        
        # Try to find each column in the header
        last_pos = 0
        for col in search_cols:
            # Look for the column in header, allowing for partial matches
            matches = [
                (pos, header_subpart) 
                for pos, header_subpart in enumerate(header_line[last_pos:].split()) 
                if col in header_subpart.upper()
            ]
            
            if matches:
                pos, header_subpart = matches[0]
                real_pos = header_line.find(header_subpart, last_pos)
                col_names.append(col)
                col_positions.append(real_pos)
                last_pos = real_pos + len(header_subpart)
        
        # Add end position
        if col_positions:
            col_positions.append(len(header_line) + 20)  # Add some padding
            
            # Create position pairs for slicing
            pos_pairs = [(col_positions[i], col_positions[i+1]) for i in range(len(col_positions)-1)]
            
            header_idx = lines.index(header_line)
            
            # Skip separator lines
            data_start = header_idx + 1
            while data_start < len(lines) and ('-' * 5) in lines[data_start]:
                data_start += 1
            
            data = []
            for line in lines[data_start:]:
                if not line.strip() or line.strip().replace('-', '').strip() == '':
                    continue
                
                row = []
                for start, end in pos_pairs:
                    value = line[start:end].strip() if start < len(line) else ""
                    row.append(value)
                
                if row and len(row) == len(col_names):
                    data.append(row)
            
            if data:
                df = pd.DataFrame(data, columns=col_names)
                return clean_numeric_columns(df, data_type)
    
    return pd.DataFrame()

def detect_and_read_file(file_path, year=None, data_type="indices"):
    """Read and parse BVMT files with year-specific handling"""
    logging.info(f"Processing {data_type} file: {file_path}")
    
    # First try parsing as mixed content
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        mixed_df = parse_mixed_content(content, year, data_type)
        if not mixed_df.empty:
            logging.info(f"Successfully parsed mixed content {data_type} file")
            return mixed_df
    except Exception as e:
        logging.debug(f"Error parsing as mixed content: {str(e)}")
    
    # Try reading as CSV with semicolon separator (for 2022+)
    if year and year >= 2022:
        try:
            df = pd.read_csv(file_path, sep=';', header=None, skipinitialspace=True)
            if not df.empty:
                if data_type == "indices":
                    df.columns = [
                        'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                        'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                        'INDICE_PLUS_BAS', 'INDICE_OUV'
                    ][:df.shape[1]]
                else:
                    df.columns = [
                        'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                        'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                        'NB_TRANSACTION', 'CAPITAUX'
                    ][:df.shape[1]]
                df = clean_numeric_columns(df, data_type)
                logging.info(f"Successfully read semicolon-separated {data_type} file")
                return df
        except Exception as e:
            logging.debug(f"Error reading as semicolon CSV: {str(e)}")
    
    # Try reading as regular CSV
    if file_path.lower().endswith('.csv'):
        try:
            df = pd.read_csv(file_path)
            if not df.empty:
                df = clean_numeric_columns(df, data_type)
                logging.info(f"Successfully read CSV {data_type} file")
                return df
        except Exception as e:
            logging.debug(f"Error reading as CSV: {str(e)}")
    
    # Try fixed-width parsing with different encodings
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
            
            df = parse_fixed_width(content, year, data_type)
            
            if not df.empty:
                df = clean_numeric_columns(df, data_type)
                logging.info(f"Successfully read {data_type} with encoding: {encoding}")
                return df
            
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.debug(f"Error with encoding {encoding}: {str(e)}")
            continue
    
    logging.error(f"Could not read {data_type} file {file_path} with any method")
    return pd.DataFrame()

def download_and_extract(year, ext, data_type="indices"):
    """Download and extract archive for a specific year with filename correction"""
    url = BASE_URL[data_type].format(year=year, ext=ext)
    
    if not verify_url_exists(url):
        logging.warning(f"URL does not exist: {url}")
        return []
    
    os.makedirs(f"raw_{data_type}", exist_ok=True)
    os.makedirs(f"extracted_{data_type}", exist_ok=True)
    
    filename = f"histo_{data_type[:-1]}_{year}.{ext}"
    raw_path = os.path.join(f"raw_{data_type}", filename)
    
    try:
        logging.info(f"Downloading {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Special handling for 2008 cotations which is actually a ZIP file
        if year == 2008 and data_type == "cotations" and ext == "rar":
            # Force treat as ZIP file
            raw_path = raw_path.replace('.rar', '.zip')
            ext = 'zip'
        
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        extracted_files = []
        if ext == 'rar':
            try:
                with rarfile.RarFile(raw_path) as rf:
                    rf.extractall(path=f"extracted_{data_type}")
                    extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in rf.namelist()]
            except rarfile.RarCannotExec:
                logging.error("RAR extraction failed - ensure unrar is installed")
                return []
            except rarfile.NotRarFile:
                # Try treating as ZIP if RAR fails (for 2008 cotations)
                try:
                    with zipfile.ZipFile(raw_path) as zf:
                        zf.extractall(path=f"extracted_{data_type}")
                        extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in zf.namelist()]
                    logging.info(f"Successfully extracted {raw_path} as ZIP file")
                except Exception as e:
                    logging.error(f"Failed to extract {raw_path} as ZIP: {str(e)}")
                    return []
        elif ext == 'zip':
            with zipfile.ZipFile(raw_path) as zf:
                zf.extractall(path=f"extracted_{data_type}")
                extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in zf.namelist()]
        
        # Fix for filename issues
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
        
        if len(file_list) > 0:
            logging.info(f"Preview of {data_type} file content:")
            try:
                with open(file_list[0], 'r', encoding='latin1', errors='replace') as f:
                    for _ in range(5):
                        line = next(f, '').strip()
                        logging.info(line)
            except Exception as e:
                logging.debug(f"Error previewing file: {str(e)}")
        
        logging.info(f"Extracted {data_type} files: {file_list}")
        return file_list
    
    except Exception as e:
        logging.error(f"Error processing {data_type} {year} ({ext}): {traceback.format_exc()}")
        return []

def try_alternate_source(year, data_type="indices"):
    """Try alternative data source for missing years"""
    url = ALTERNATE_URL[data_type].format(year=year)
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            dfs = pd.read_html(StringIO(response.text))
            if dfs:
                df = dfs[0]
                df.columns = df.iloc[0]
                df = df[1:]
                df = clean_numeric_columns(df, data_type)
                return df
    except Exception as e:
        logging.warning(f"Failed to get alternate {data_type} data for {year}: {str(e)}")
    return pd.DataFrame()

def clean_consolidated_data(df, data_type="indices"):
    """Clean the DataFrame by removing problematic rows and fixing data types"""
    if data_type == "indices":
        # Remove rows that contain column headers in data fields for indices
        column_headers = ['CODE_INDICE', 'LIB_INDICE', 'SEANCE', 'INDICE_JOUR', 
                         'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                         'INDICE_PLUS_BAS', 'INDICE_OUV']
        
        # Clean numeric columns again to ensure consistency
        numeric_cols = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                       'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
    else:
        # Remove rows that contain column headers in data fields for cotations
        column_headers = ['SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                         'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                         'NB_TRANSACTION', 'CAPITAUX']
        
        # Clean numeric columns again to ensure consistency
        numeric_cols = ['OUVERTURE', 'CLOTURE', 'PLUS_BAS', 'PLUS_HAUT',
                       'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
    
    # Create a mask to identify rows that contain any column header in their data
    mask = df.apply(lambda row: any(str(header).lower() in str(cell).lower() 
                   for cell in row for header in column_headers), axis=1)
    
    # Keep only rows that don't match the mask
    df = df[~mask]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Clean date column - remove time components if they exist
    if 'SEANCE' in df.columns:
        df['SEANCE'] = df['SEANCE'].astype(str).str.replace(r'\s+\d{1,2}:\d{2}:\d{2}', '', regex=True).str.strip()
        df['SEANCE'] = pd.to_datetime(df['SEANCE'], errors='coerce')
        df = df.dropna(subset=['SEANCE'])
    
    # Clean CODE column by stripping whitespace
    code_col = 'CODE_INDICE' if data_type == 'indices' else 'CODE'
    if code_col in df.columns:
        df[code_col] = df[code_col].astype(str).str.strip()
    
    # Remove any completely empty rows
    df = df.dropna(how='all')
    
    return df

def process_year(year, data_type="indices"):
    """Process data for a specific year and data type with enhanced error handling"""
    os.makedirs(f"csv_{data_type}", exist_ok=True)
    csv_filename = f"{year}_histo_{data_type}.csv"
    csv_path = os.path.join(f"csv_{data_type}", csv_filename)
    
    for ext in ['rar', 'zip']:
        extracted_files = download_and_extract(year, ext, data_type)
        
        if not extracted_files:
            logging.warning(f"No {data_type} files found for {year} ({ext})")
            continue
        
        for source_file in extracted_files:
            try:
                df = detect_and_read_file(source_file, year, data_type)
                
                if not df.empty:
                    # Apply cleaning before saving
                    df = clean_consolidated_data(df, data_type)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    logging.info(f"Successfully saved {data_type} {csv_path}")
                    return True
                else:
                    logging.warning(f"Empty {data_type} DataFrame for {source_file}")
            except Exception as e:
                logging.error(f"Error processing {data_type} {source_file}: {traceback.format_exc()}")
    
    logging.info(f"Trying alternate source for {data_type} year {year}")
    df = try_alternate_source(year, data_type)
    if not df.empty:
        df = clean_consolidated_data(df, data_type)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logging.info(f"Saved {data_type} from alternate source: {csv_path}")
        return True
    
    logging.warning(f"Could not process {data_type} data for year {year}")
    return False

def generate_consolidated_csv(years, data_type="indices"):
    """Combine all yearly data into one consolidated CSV with cleaning"""
    dfs = []
    for year in years:
        csv_path = os.path.join(f"csv_{data_type}", f"{year}_histo_{data_type}.csv")
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                
                # Clean the DataFrame before adding
                df = clean_consolidated_data(df, data_type)
                
                df['YEAR'] = year
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error reading {data_type} {csv_path}: {str(e)}")
    
    if dfs:
        consolidated = pd.concat(dfs, ignore_index=True)
        
        # Additional cleaning for the consolidated DataFrame
        consolidated = clean_consolidated_data(consolidated, data_type)
        
        # Ensure consistent column ordering
        if data_type == "indices":
            desired_columns = [
                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                'INDICE_PLUS_BAS', 'INDICE_OUV', 'YEAR'
            ]
        else:
            desired_columns = [
                'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                'NB_TRANSACTION', 'CAPITAUX', 'YEAR'
            ]
        
        # Only keep columns that exist in the DataFrame
        available_columns = [col for col in desired_columns if col in consolidated.columns]
        consolidated = consolidated[available_columns]
        
        os.makedirs(os.path.join("..", "data", "input", data_type), exist_ok=True)
        consolidated_path = os.path.join("..", "data", "input", data_type, f"{data_type}.csv")
        consolidated.to_csv(consolidated_path, index=False)
        
        file_size = os.path.getsize(consolidated_path) / (1024 * 1024)
        logging.info(f"Created consolidated {data_type} file: {consolidated_path}")
        logging.info(f"Consolidated {data_type} file size: {file_size:.2f} MB")
        
        return file_size
    return 0

def print_summary(start_time, successful_years, failed_years, data_types):
    """Print a nice summary of the execution"""
    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)
    
    print("\n" + "="*60)
    print("SCRIPT EXECUTION SUMMARY FOR BVMT DATA 2008 --> 2024".center(60))
    print("="*60)
    print(f"{'Total processing time:':<30} {int(minutes)} minutes {seconds:.2f} seconds")
    
    for data_type in data_types:
        print(f"\n{data_type.upper()}:")
        print(f"{'Successful years:':<30} {len(successful_years[data_type])}")
        print(f"{'Failed years:':<30} {len(failed_years[data_type])}")
        print(f"{'File size:':<30} {data_types[data_type]:.2f} MB")
        print("\nSuccessful years:", successful_years[data_type])
        print("Failed years:", failed_years[data_type])
    
    print("="*60 + "\n")

def main():
    """Main processing function"""
    start_time = time.time()
    
    successful_years = {
        "indices": [],
        "cotations": []
    }
    
    failed_years = {
        "indices": [],
        "cotations": []
    }
    
    data_types = {
        "indices": 0,
        "cotations": 0
    }
    
    start_year = 2008
    end_year = 2024
    
    # Process indices first
    for year in range(start_year, end_year + 1):
        logging.info(f"\n{'='*40}\nProcessing indices for year: {year}\n{'='*40}")
        if process_year(year, "indices"):
            successful_years["indices"].append(year)
        else:
            failed_years["indices"].append(year)
    
    # Then process cotations
    for year in range(start_year, end_year + 1):
        logging.info(f"\n{'='*40}\nProcessing cotations for year: {year}\n{'='*40}")
        if process_year(year, "cotations"):
            successful_years["cotations"].append(year)
        else:
            failed_years["cotations"].append(year)
    
    # Generate consolidated files
    data_types["indices"] = generate_consolidated_csv(successful_years["indices"], "indices")
    data_types["cotations"] = generate_consolidated_csv(successful_years["cotations"], "cotations")
    
    print_summary(start_time, successful_years, failed_years, data_types)
    return successful_years, failed_years

if __name__ == "__main__":
    successful_years, failed_years = main()