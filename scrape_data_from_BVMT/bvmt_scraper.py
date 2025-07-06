import os
import requests
import rarfile
import zipfile
import pandas as pd
import logging
import traceback
import time
import re
import logging.handlers
from io import StringIO
from datetime import datetime
from colorlog import ColoredFormatter

# Configure professional logging
def setup_logging():
    """Configure professional logging with colorized output and file rotation"""
    os.makedirs('logs', exist_ok=True)
    
    console_format = (
        '%(log_color)s%(asctime)s - %(levelname)-8s%(reset)s | '
        '%(log_color)s%(module)-10s%(reset)s | '
        '%(log_color)s%(message)s%(reset)s'
    )
    
    file_format = (
        '%(asctime)s - %(levelname)-8s | '
        '%(module)-10s | '
        '%(message)s'
    )
    
    color_formatter = ColoredFormatter(
        console_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )
    
    file_formatter = logging.Formatter(file_format, datefmt='%Y-%m-%d %H:%M:%S')
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    
    file_handler = logging.handlers.RotatingFileHandler(
        filename='logs/bvmt_scraper.log',
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[console_handler, file_handler]
    )
    
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

setup_logging()
logger = logging.getLogger(__name__)

BASE_URL = {
    "indices": "https://www.bvmt.com.tn/sites/default/files/historiques/indices/histo_indice_{year}.{ext}",
    "cotations": "https://www.bvmt.com.tn/sites/default/files/historiques/cotations/histo_cotation_{year}.{ext}"
}

ALTERNATE_URL = {
    "indices": "https://www.bvmt.com.tn/backoffice/historiques/indices_archive.php?annee={year}",
    "cotations": "https://www.bvmt.com.tn/backoffice/historiques/cotations_archive.php?annee={year}"
}

def verify_url_exists(url):
    """Verify if a URL exists with detailed logging"""
    try:
        logger.debug(f"Verifying URL: {url}")
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            logger.debug(f"URL exists: {url}")
            return True
        logger.debug(f"URL returned status {response.status_code}: {url}")
        return False
    except requests.RequestException as e:
        logger.debug(f"URL verification failed for {url}: {str(e)}", exc_info=True)
        return False

def clean_numeric_columns(df, data_type="indices"):
    """Convert numeric columns from European format to float with enhanced handling"""
    logger.debug(f"Cleaning numeric columns for {data_type} data")
    
    df.columns = df.columns.str.strip()
    
    if data_type == "indices":
        col_mapping = {
            'SEANCE': ['Date', 'SEANCE', 'Seance'],
            'GROUPE': ['GROUPE', 'C_GR_RLC', 'GROUP', 'Groupe'],
            'CODE_INDICE': ['CODE_INDICE', 'Code', 'Index Code', 'CODE_IND'],
            'LIB_INDICE': ['LIB_INDICE', 'Libellé', 'Index Name', 'LIB_IND'],
            'INDICE_JOUR': ['INDICE_JOUR', 'Clôture', 'Closing'],
            'INDICE_VEILLE': ['INDICE_VEILLE', 'Clôture veille', 'Previous Close', 'INDICE_VEILLE'],
            'VARIATION_VEILLE': ['VARIATION_VEILLE', 'Var. %', 'Variation', 'Change %'],
            'INDICE_PLUS_HAUT': ['INDICE_PLUS_HAUT', 'Plus haut', 'High'],
            'INDICE_PLUS_BAS': ['INDICE_PLUS_BAS', 'Plus bas', 'Low'],
            'INDICE_OUV': ['INDICE_OUV', 'Ouverture', 'Open']
        }
        
        numeric_cols = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE', 'GROUPE',
                       'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
    else:
        col_mapping = {
            'SEANCE': ['SEANCE', 'Date'],
            'GROUPE': ['GROUPE', 'C_GR_RLC', 'GROUP', 'Groupe'],
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
        
        numeric_cols = ['OUVERTURE', 'CLOTURE', 'PLUS_BAS', 'PLUS_HAUT', 
                        'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
    
    new_columns = {}
    for standard_name, variants in col_mapping.items():
        for variant in variants:
            if variant in df.columns:
                logger.debug(f"Mapping column {variant} -> {standard_name}")
                new_columns[standard_name] = df[variant]
                break
    
    if new_columns:
        logger.debug("Reconstructing DataFrame with standardized columns")
        df = pd.DataFrame(new_columns)
    
    for col in numeric_cols:
        if col in df.columns:
            logger.debug(f"Processing numeric column: {col}")
            df[col] = df[col].astype(str).str.replace(r'[^\d,.-]', '', regex=True)
            df[col] = df[col].str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'SEANCE' in df.columns:
        logger.debug("Processing date column (SEANCE)")
        df['SEANCE'] = df['SEANCE'].astype(str).str.replace(r'\s+\d{1,2}:\d{2}:\d{2}', '', regex=True).str.strip()
        
        date_formats = ['%d/%m/%Y', '%d%m%Y', '%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%d/%m/%y', '%d%m%y']
        
        for date_format in date_formats:
            try:
                df['SEANCE'] = pd.to_datetime(
                    df['SEANCE'].astype(str).str.strip(),
                    format=date_format,
                    errors='raise'
                )
                logger.debug(f"Successfully parsed dates with format: {date_format}")
                break
            except ValueError:
                continue
        else:
            logger.debug("Could not parse dates with specific formats, using coerce")
            df['SEANCE'] = pd.to_datetime(df['SEANCE'], errors='coerce')
    
    logger.debug(f"Finished cleaning {data_type} data. Shape: {df.shape}")
    return df

def parse_quoted_cotations(content):
    """Special parser for 2018/2019 cotations with quoted values"""
    logger.debug("Processing quoted cotations format")
    
    lines = [line.strip() for line in content.split('\n') if line.strip()]
    data = []
    
    for line in lines:
        if not line or line.startswith(('SEANCE', 'Date')) or line.replace('-', '').strip() == '':
            continue
            
        # Handle quoted sections that contain commas
        processed_line = []
        in_quote = False
        current_field = ''
        
        for char in line:
            if char == '"':
                in_quote = not in_quote
            elif char == ',' and not in_quote:
                processed_line.append(current_field.strip())
                current_field = ''
            else:
                current_field += char
                
        processed_line.append(current_field.strip())
        
        # Clean up each field
        processed_line = [field.replace('"', '').strip() for field in processed_line]
        
        # Handle cases where GROUPE might be empty
        if len(processed_line) >= 10:
            data.append(processed_line)
        elif len(processed_line) >= 9:
            # Insert empty GROUPE if missing
            processed_line.insert(1, '')
            data.append(processed_line)
    
    if not data:
        return pd.DataFrame()
    
    columns = [
        'SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
        'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX'
    ]
    
    df = pd.DataFrame(data, columns=columns[:len(data[0])])
    return clean_numeric_columns(df, "cotations")

def parse_fixed_width_2015(file_path, data_type="indices"):
    """Parse the 2015 fixed-width file format specifically with fixes for cotations data"""
    logger.debug(f"Parsing 2015 fixed-width file format: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='latin1', errors='replace') as f:
            content = f.read()
            
        # Special handling for 2015 cotations that are actually CSV
        if data_type == "cotations" and ',' in content.split('\n')[0]:
            logger.debug("2015 cotations appears to be CSV format")
            
            # Handle the mixed GROUPE/CODE issue in 2015 data
            lines = content.split('\n')
            cleaned_lines = []
            
            for line in lines:
                if not line.strip():
                    continue
                    
                # Handle cases where GROUPE and CODE are merged (e.g., "11     10,10,MONOPRIX")
                if re.match(r'\d{4}-\d{2}-\d{2},\d+\s+\d+,\d+', line):
                    parts = line.split(',')
                    # Split the merged GROUPE/CODE part
                    groupe_code = parts[1].strip().split()
                    if len(groupe_code) == 2:
                        # Reconstruct the line with proper columns
                        new_line = f"{parts[0]},{groupe_code[0]},{groupe_code[1]},{','.join(parts[2:])}"
                        cleaned_lines.append(new_line)
                    else:
                        cleaned_lines.append(line)
                else:
                    cleaned_lines.append(line)
            
            # Recreate the content with cleaned lines
            content = '\n'.join(cleaned_lines)
            
            # Now parse as CSV
            df = pd.read_csv(StringIO(content))
            return clean_numeric_columns(df, data_type)
            
        lines = [line for line in content.split('\n') if line.strip()]
        
        # Special handling for cotations fixed-width format
        if data_type == "cotations":
            # Find header line
            header_line = None
            for line in lines:
                if 'SEANCE' in line and ('CODE' in line or 'VALEUR' in line):
                    header_line = line
                    break
            
            if not header_line:
                logger.warning("Could not find header line in 2015 cotations file")
                return pd.DataFrame()
            
            # Define column positions based on the fixed-width format
            # These positions are specifically tuned for the 2015 format
            col_positions = [
                (0, 10),    # SEANCE
                (10, 20),   # GROUPE
                (20, 30),   # CODE
                (30, 50),   # VALEUR
                (50, 60),   # OUVERTURE
                (60, 70),   # CLOTURE
                (70, 80),   # PLUS_BAS
                (80, 90),   # PLUS_HAUT
                (90, 105),  # QUANTITE_NEGOCIEE
                (105, 120), # NOMBRE_TRANSACTION
                (120, 135)  # CAPITAUX
            ]
            
            columns = [
                'SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX'
            ]
            
            # Find data separator line
            separator_idx = -1
            for i, line in enumerate(lines):
                if '-' * 5 in line and i > lines.index(header_line):
                    separator_idx = i
                    break
            
            if separator_idx == -1:
                logger.warning("Could not find separator line in 2015 cotations file")
                return pd.DataFrame()
            
            # Process data lines
            data = []
            for line in lines[separator_idx + 1:]:
                if line.strip() and not line.startswith('------'):
                    row = []
                    for start, end in col_positions:
                        if start < len(line):
                            value = line[start:end].strip() if end <= len(line) else line[start:].strip()
                            row.append(value)
                        else:
                            row.append("")
                    
                    # Special handling for GROUPE/CODE values
                    if len(row) > 1 and row[1]:  # If GROUPE exists
                        # Sometimes CODE is merged with GROUPE
                        if ' ' in row[1]:
                            parts = row[1].split()
                            if len(parts) == 2:
                                row[1] = parts[0]  # GROUPE
                                if len(row) > 2:
                                    row[2] = parts[1]  # CODE
                    
                    if len(row) >= 3 and row[0].strip() and row[2].strip():  # Check SEANCE and CODE
                        data.append(row[:len(columns)])
            
            if not data:
                logger.warning("No data rows found in 2015 cotations file")
                return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=columns[:len(data[0])])
            
            # Clean numeric columns that might have comma as decimal separator
            for col in ['OUVERTURE', 'CLOTURE', 'PLUS_BAS', 'PLUS_HAUT', 'CAPITAUX']:
                if col in df.columns:
                    df[col] = df[col].str.replace(',', '.').astype(float, errors='ignore')
            
            # Clean date format
            if 'SEANCE' in df.columns:
                df['SEANCE'] = pd.to_datetime(df['SEANCE'], format='%d/%m/%y', errors='coerce')
            
            logger.info(f"Successfully parsed 2015 cotations fixed-width file, found {len(df)} records")
            return df
        
        # Original indices processing remains the same
        header_line = None
        for line in lines:
            if 'SEANCE' in line and ('CODE_IND' in line or 'LIB_IND' in line):
                header_line = line
                break
        
        if not header_line:
            logger.warning("Could not find header line in 2015 file")
            return pd.DataFrame()
        
        positions = []
        current_pos = 0
        headers = header_line.strip().split()
        
        for header in headers:
            pos = header_line.find(header, current_pos)
            if pos >= 0:
                positions.append(pos)
                current_pos = pos + len(header)
        
        positions.append(len(header_line))
        
        columns = ['SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR', 'INDICE_VEILLE', 
                  'VARIATION_VEILLE', 'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
        
        separator_idx = -1
        for i, line in enumerate(lines):
            if '-' * 5 in line and i > lines.index(header_line):
                separator_idx = i
                break
        
        if separator_idx == -1:
            logger.warning("Could not find separator line in 2015 file")
            return pd.DataFrame()
        
        data = []
        for line in lines[separator_idx + 1:]:
            if line.strip() and not line.startswith('------'):
                row = []
                for i in range(len(positions) - 1):
                    start = positions[i]
                    end = positions[i + 1]
                    if start < len(line):
                        value = line[start:end].strip() if end <= len(line) else line[start:].strip()
                        row.append(value)
                    else:
                        row.append("")
                
                if len(row) >= 3 and row[0].strip() and row[1].strip():
                    while len(row) < len(columns):
                        row.append("")
                    data.append(row[:len(columns)])
        
        if not data:
            logger.warning("No data rows found in 2015 file")
            return pd.DataFrame()
        
        df = pd.DataFrame(data, columns=columns[:len(data[0])])
        df = clean_numeric_columns(df, data_type)
        logger.info(f"Successfully parsed 2015 fixed-width file, found {len(df)} records")
        return df
        
    except Exception as e:
        logger.error(f"Error parsing 2015 fixed-width file: {str(e)}", exc_info=True)
        return pd.DataFrame()

def parse_mixed_content(content, year=None, data_type="indices"):
    """Parse files that contain mixed formats with detailed logging"""
    logger.debug(f"Parsing mixed content for {data_type} year {year}")
    
    # Special handling for quoted cotations (2018/2019)
    if data_type == "cotations" and year in [2018, 2019] and '"' in content:
        logger.debug("Detected quoted cotations format (2018/2019)")
        return parse_quoted_cotations(content)
    
    sections = []
    current_section = []
    
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
        if any(';' in line for line in section.split('\n')):
            try:
                logger.debug("Found semicolon-delimited section")
                cleaned_lines = []
                for line in section.split('\n'):
                    if ';' in line:
                        cleaned_line = re.sub(r'\s*;\s*', ';', line.strip())
                        cleaned_lines.append(cleaned_line)
                
                if cleaned_lines:
                    df = pd.read_csv(StringIO('\n'.join(cleaned_lines)), sep=';', header=None)
                    if not df.empty:
                        if data_type == "indices":
                            if df.shape[1] >= 9:
                                df.columns = [
                                    'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                                    'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                                    'INDICE_PLUS_BAS', 'INDICE_OUV'
                                ][:df.shape[1]]
                        else:
                            if df.shape[1] >= 7:
                                columns = [
                                    'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                                    'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                                    'NB_TRANSACTION', 'CAPITAUX'
                                ]
                                if df.shape[1] >= 10:
                                    columns.insert(1, 'GROUPE')
                                df.columns = columns[:df.shape[1]]
                        dfs.append(df)
            except Exception as e:
                logger.debug(f"Failed to parse semicolon section: {str(e)}", exc_info=True)
        
        elif any(',' in line for line in section.split('\n')):
            try:
                logger.debug("Found comma-delimited section")
                df = pd.read_csv(StringIO(section), header=None)
                if not df.empty:
                    if data_type == "indices":
                        if df.shape[1] >= 9:
                            df.columns = [
                                'SEANCE', 'INDICE_JOUR', 'CODE_INDICE', 'INDICE_VEILLE',
                                'VARIATION_VEILLE', 'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS',
                                'INDICE_OUV', 'YEAR'
                            ][:df.shape[1]]
                    else:
                        if df.shape[1] >= 7:
                            columns = [
                                'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE',
                                'NB_TRANSACTION', 'CAPITAUX'
                            ]
                            if df.shape[1] >= 11:
                                columns.insert(1, 'GROUPE')
                            df.columns = columns[:df.shape[1]]
                    dfs.append(df)
            except Exception as e:
                logger.debug(f"Failed to parse comma section: {str(e)}", exc_info=True)
    
    if dfs:
        logger.debug(f"Combining {len(dfs)} sections for {data_type}")
        combined_df = pd.concat(dfs, ignore_index=True)
        return clean_numeric_columns(combined_df, data_type)
    
    logger.debug("No valid sections found in mixed content")
    return pd.DataFrame()

def parse_fixed_width(content, year=None, data_type="indices"):
    """Parse the fixed-width file format with detailed logging"""
    logger.debug(f"Parsing fixed-width content for {data_type} year {year}")
    
    lines = [line for line in content.split('\n') if line.strip()]
    
    mixed_df = parse_mixed_content(content, year, data_type)
    if not mixed_df.empty:
        logger.debug("Fixed-width content actually contained mixed formats")
        return mixed_df
    
    if data_type == "cotations":
        logger.debug("Processing cotations fixed-width format")
        header_line = next((line for line in lines if 'SEANCE' in line and ('CODE' in line or 'VALEUR' in line)), None)
        
        if not header_line:
            header_line = next((line for line in lines if any(term in line for term in 
                               ['SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE'])), None)
        
        if header_line:
            logger.debug(f"Found header line: {header_line}")
            columns = []
            column_positions = []
            
            search_cols = ['SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE', 'PLUS_BAS', 
                          'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
            
            last_pos = 0
            for col in search_cols:
                pos = header_line.find(col, last_pos)
                if pos >= 0:
                    columns.append(col)
                    column_positions.append(pos)
                    last_pos = pos + len(col)
            
            column_positions.append(len(header_line))
            
            if len(columns) >= 3:
                logger.debug(f"Identified columns: {columns}")
                col_positions = [(column_positions[i], column_positions[i+1]) 
                                for i in range(len(column_positions)-1)]
                
                header_idx = lines.index(header_line)
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
                            if start >= len(line):
                                value = ""
                            elif end > len(line):
                                value = line[start:].strip()
                            else:
                                value = line[start:end].strip()
                            row.append(value)
                    except Exception as e:
                        logger.debug(f"Error parsing line: {line}, error: {str(e)}")
                        continue
                    
                    if row and len(row) == len(columns):
                        data.append(row)
                
                if data:
                    logger.debug(f"Successfully parsed {len(data)} rows")
                    df = pd.DataFrame(data, columns=columns)
                    return clean_numeric_columns(df, data_type)
    
    logger.debug("Processing generic fixed-width format")
    header_line = next((line for line in lines if 'SEANCE' in line), None)
    if header_line:
        logger.debug(f"Found header line: {header_line}")
        col_names = []
        col_positions = []
        
        if data_type == "indices":
            search_cols = [
                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                'INDICE_PLUS_BAS', 'INDICE_OUV'
            ]
            search_cols.extend(['CODE_IND', 'LIB_IND'])
        else:
            search_cols = [
                'SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE', 
                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                'NB_TRANSACTION', 'CAPITAUX'
            ]
        
        last_pos = 0
        for col in search_cols:
            matches = [
                (pos, header_subpart) 
                for pos, header_subpart in enumerate(header_line[last_pos:].split()) 
                if col in header_subpart.upper()
            ]
            
            if matches:
                pos, header_subpart = matches[0]
                real_pos = header_line.find(header_subpart, last_pos)
                
                if col == 'CODE_IND':
                    col = 'CODE_INDICE'
                elif col == 'LIB_IND':
                    col = 'LIB_INDICE'
                
                col_names.append(col)
                col_positions.append(real_pos)
                last_pos = real_pos + len(header_subpart)
        
        if col_positions:
            col_positions.append(len(header_line) + 20)
            pos_pairs = [(col_positions[i], col_positions[i+1]) for i in range(len(col_positions)-1)]
            
            header_idx = lines.index(header_line)
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
                logger.debug(f"Successfully parsed {len(data)} rows")
                df = pd.DataFrame(data, columns=col_names)
                return clean_numeric_columns(df, data_type)
    
    logger.debug("Could not parse fixed-width content")
    return pd.DataFrame()

def detect_and_read_file(file_path, year=None, data_type="indices"):
    """Read and parse BVMT files with detailed logging"""
    logger.info(f"Processing {data_type} file: {file_path}")
    
    if year == 2015 and data_type == "indices":
        logger.debug("Trying specialized 2015 fixed-width parser")
        df = parse_fixed_width_2015(file_path, data_type)
        if not df.empty:
            logger.info("Successfully parsed 2015 data with specialized parser")
            return df
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        
        # Special handling for quoted cotations (2018/2019)
        if data_type == "cotations" and year in [2018, 2019] and '"' in content:
            logger.debug("Detected quoted cotations format (2018/2019)")
            df = parse_quoted_cotations(content)
            if not df.empty:
                return df
        
        mixed_df = parse_mixed_content(content, year, data_type)
        if not mixed_df.empty:
            logger.info(f"Successfully parsed mixed content {data_type} file")
            return mixed_df
    except Exception as e:
        logger.debug(f"Error parsing as mixed content: {str(e)}", exc_info=True)
    
    if year and year >= 2022:
        try:
            logger.debug("Trying semicolon-separated CSV format")
            df = pd.read_csv(file_path, sep=';', header=None, skipinitialspace=True)
            if not df.empty:
                if data_type == "indices":
                    df.columns = [
                        'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                        'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                        'INDICE_PLUS_BAS', 'INDICE_OUV'
                    ][:df.shape[1]]
                else:
                    if df.shape[1] >= 11:
                        df.columns = [
                            'SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                            'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                            'NB_TRANSACTION', 'CAPITAUX'
                        ][:df.shape[1]]
                    else:
                        df.columns = [
                            'SEANCE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                            'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                            'NB_TRANSACTION', 'CAPITAUX'
                        ][:df.shape[1]]
                df = clean_numeric_columns(df, data_type)
                logger.info(f"Successfully read semicolon-separated {data_type} file")
                return df
        except Exception as e:
            logger.debug(f"Error reading as semicolon CSV: {str(e)}", exc_info=True)
    
    if file_path.lower().endswith('.csv'):
        try:
            logger.debug("Trying standard CSV format")
            df = pd.read_csv(file_path)
            if not df.empty:
                df = clean_numeric_columns(df, data_type)
                logger.info(f"Successfully read CSV {data_type} file")
                return df
        except Exception as e:
            logger.debug(f"Error reading as CSV: {str(e)}", exc_info=True)
    
    encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            logger.debug(f"Trying fixed-width with encoding: {encoding}")
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
            
            df = parse_fixed_width(content, year, data_type)
            
            if not df.empty:
                df = clean_numeric_columns(df, data_type)
                logger.info(f"Successfully read {data_type} with encoding: {encoding}")
                return df
            
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.debug(f"Error with encoding {encoding}: {str(e)}", exc_info=True)
            continue
    
    logger.error(f"Could not read {data_type} file {file_path} with any method")
    return pd.DataFrame()

def download_and_extract(year, ext, data_type="indices"):
    """Download and extract archive with detailed logging"""
    url = BASE_URL[data_type].format(year=year, ext=ext)
    logger.info(f"Attempting to download {data_type} for {year} from {url}")
    
    if not verify_url_exists(url):
        logger.warning(f"URL does not exist: {url}")
        return []
    
    os.makedirs(f"raw_{data_type}", exist_ok=True)
    os.makedirs(f"extracted_{data_type}", exist_ok=True)
    
    filename = f"histo_{data_type[:-1]}_{year}.{ext}"
    raw_path = os.path.join(f"raw_{data_type}", filename)
    
    try:
        logger.info(f"Downloading {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        if year == 2008 and data_type == "cotations" and ext == "rar":
            raw_path = raw_path.replace('.rar', '.zip')
            ext = 'zip'
            logger.info(f"Adjusting file type for 2008 cotations (RAR->ZIP)")
        
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded {data_type} for {year} to {raw_path}")
        
        extracted_files = []
        if ext == 'rar':
            try:
                logger.debug("Attempting RAR extraction")
                with rarfile.RarFile(raw_path) as rf:
                    rf.extractall(path=f"extracted_{data_type}")
                    extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in rf.namelist()]
                logger.info(f"Extracted {len(extracted_files)} files from RAR archive")
            except rarfile.RarCannotExec:
                logger.error("RAR extraction failed - ensure unrar is installed")
                return []
            except rarfile.NotRarFile:
                logger.debug("File is not actually RAR, trying as ZIP")
                try:
                    with zipfile.ZipFile(raw_path) as zf:
                        zf.extractall(path=f"extracted_{data_type}")
                        extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in zf.namelist()]
                    logger.info(f"Successfully extracted {raw_path} as ZIP file")
                except Exception as e:
                    logger.error(f"Failed to extract {raw_path} as ZIP: {str(e)}", exc_info=True)
                    return []
        elif ext == 'zip':
            logger.debug("Attempting ZIP extraction")
            with zipfile.ZipFile(raw_path) as zf:
                zf.extractall(path=f"extracted_{data_type}")
                extracted_files = [os.path.join(f"extracted_{data_type}", f) for f in zf.namelist()]
            logger.info(f"Extracted {len(extracted_files)} files from ZIP archive")
        
        corrected_files = []
        for f in extracted_files:
            if '20015' in f:
                corrected = f.replace('20015', '2015')
                os.rename(f, corrected)
                corrected_files.append(corrected)
                logger.info(f"Corrected filename from {f} to {corrected}")
            else:
                corrected_files.append(f)
        
        valid_extensions = ('.txt', '.csv')
        file_list = [f for f in corrected_files if f.lower().endswith(valid_extensions)]
        
        if file_list:
            logger.debug(f"Preview of {data_type} file content:")
            try:
                with open(file_list[0], 'r', encoding='latin1', errors='replace') as f:
                    for i in range(5):
                        line = next(f, '').strip()
                        if line:
                            logger.debug(f"Line {i+1}: {line}")
            except Exception as e:
                logger.debug(f"Error previewing file: {str(e)}", exc_info=True)
        
        logger.info(f"Extracted {len(file_list)} {data_type} files for year {year}")
        return file_list
    
    except Exception as e:
        logger.error(f"Error processing {data_type} {year} ({ext}): {str(e)}", exc_info=True)
        return []

def try_alternate_source(year, data_type="indices"):
    """Try alternative data source with detailed logging"""
    url = ALTERNATE_URL[data_type].format(year=year)
    logger.info(f"Attempting alternate source for {data_type} {year} at {url}")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            logger.debug("Got successful response from alternate source")
            dfs = pd.read_html(StringIO(response.text))
            if dfs:
                df = dfs[0]
                df.columns = df.iloc[0]
                df = df[1:]
                df = clean_numeric_columns(df, data_type)
                logger.info(f"Successfully retrieved {data_type} data from alternate source")
                return df
            else:
                logger.warning("No tables found in alternate source response")
        else:
            logger.warning(f"Alternate source returned status {response.status_code}")
    except Exception as e:
        logger.warning(f"Failed to get alternate {data_type} data for {year}: {str(e)}", exc_info=True)
    return pd.DataFrame()

def clean_consolidated_data(df, data_type="indices"):
    """Clean the DataFrame with detailed logging"""
    logger.debug(f"Cleaning consolidated {data_type} data")
    
    if data_type == "indices":
        column_headers = ['CODE_INDICE', 'LIB_INDICE', 'SEANCE', 'INDICE_JOUR', 
                         'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                         'INDICE_PLUS_BAS', 'INDICE_OUV']
        numeric_cols = ['INDICE_JOUR', 'INDICE_VEILLE', 'VARIATION_VEILLE',
                       'INDICE_PLUS_HAUT', 'INDICE_PLUS_BAS', 'INDICE_OUV']
    else:
        column_headers = ['SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                         'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                         'NB_TRANSACTION', 'CAPITAUX']
        numeric_cols = ['OUVERTURE', 'CLOTURE', 'PLUS_BAS', 'PLUS_HAUT',
                       'QUANTITE_NEGOCIEE', 'NB_TRANSACTION', 'CAPITAUX']
    
    mask = df.apply(lambda row: any(str(header).lower() in str(cell).lower() 
               for cell in row for header in column_headers), axis=1)
    
    rows_before = len(df)
    df = df[~mask]
    rows_removed = rows_before - len(df)
    if rows_removed > 0:
        logger.debug(f"Removed {rows_removed} rows containing column headers")
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'SEANCE' in df.columns:
        df['SEANCE'] = df['SEANCE'].astype(str).str.replace(r'\s+\d{1,2}:\d{2}:\d{2}', '', regex=True).str.strip()
        df['SEANCE'] = pd.to_datetime(df['SEANCE'], errors='coerce')
        df = df.dropna(subset=['SEANCE'])
    
    code_col = 'CODE_INDICE' if data_type == 'indices' else 'CODE'
    if code_col in df.columns:
        df[code_col] = df[code_col].astype(str).str.strip()
    
    rows_before = len(df)
    df = df.dropna(how='all')
    rows_removed = rows_before - len(df)
    if rows_removed > 0:
        logger.debug(f"Removed {rows_removed} completely empty rows")
    
    logger.debug(f"Final cleaned {data_type} data shape: {df.shape}")
    return df

def process_year(year, data_type="indices"):
    """Process data for a specific year with detailed logging and fixes for 2023/2015 issues"""
    logger.info(f"\n{'='*40}\nProcessing {data_type} for year: {year}\n{'='*40}")
    
    os.makedirs(f"csv_{data_type}", exist_ok=True)
    csv_filename = f"{year}_histo_{data_type}.csv"
    csv_path = os.path.join(f"csv_{data_type}", csv_filename)
    
    for ext in ['rar', 'zip']:
        extracted_files = download_and_extract(year, ext, data_type)
        
        if not extracted_files:
            logger.warning(f"No {data_type} files found for {year} ({ext})")
            continue
        
        for source_file in extracted_files:
            try:
                # Special handling for 2023 cotations mislabeled as 2022
                if year == 2023 and data_type == "cotations" and "2022" in source_file:
                    logger.info(f"Correcting mislabeled 2022 file for 2023 cotations: {source_file}")
                    corrected_file = source_file.replace("2022", "2023")
                    os.rename(source_file, corrected_file)
                    source_file = corrected_file
                    logger.info(f"Renamed file to: {corrected_file}")
                
                logger.debug(f"Processing source file: {source_file}")
                df = detect_and_read_file(source_file, year, data_type)
                
                if not df.empty:
                    df = clean_consolidated_data(df, data_type)
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    logger.info(f"Successfully saved {data_type} to {csv_path}")
                    return True
                else:
                    logger.warning(f"Empty {data_type} DataFrame for {source_file}")
            except Exception as e:
                logger.error(f"Error processing {data_type} {source_file}: {str(e)}", exc_info=True)
    
    if year == 2015 and data_type == "indices":
        logger.info("Attempting manual handling for 2015 indices data")
        for root, dirs, files in os.walk(f"extracted_{data_type}"):
            for file in files:
                if "2015" in file and file.endswith(('.txt', '.csv')):
                    file_path = os.path.join(root, file)
                    logger.info(f"Found potential 2015 indices file: {file_path}")
                    df = parse_fixed_width_2015(file_path, data_type)
                    if not df.empty:
                        df = clean_consolidated_data(df, data_type)
                        df.to_csv(csv_path, index=False, encoding='utf-8')
                        logger.info(f"Successfully saved 2015 {data_type} to {csv_path}")
                        return True
    
    logger.info(f"Trying alternate source for {data_type} year {year}")
    df = try_alternate_source(year, data_type)
    if not df.empty:
        df = clean_consolidated_data(df, data_type)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"Saved {data_type} from alternate source: {csv_path}")
        return True
    
    logger.warning(f"Could not process {data_type} data for year {year}")
    return False

def generate_consolidated_csv(years, data_type="indices"):
    """Combine all yearly data into one consolidated CSV with detailed logging"""
    logger.info(f"\n{'='*40}\nGenerating consolidated {data_type} CSV\n{'='*40}")
    
    dfs = []
    for year in years:
        csv_path = os.path.join(f"csv_{data_type}", f"{year}_histo_{data_type}.csv")
        if os.path.exists(csv_path):
            try:
                logger.debug(f"Loading {data_type} data for {year}")
                df = pd.read_csv(csv_path)
                df = clean_consolidated_data(df, data_type)
                dfs.append(df)
                logger.debug(f"Loaded {data_type} data for {year} with shape {df.shape}")
            except Exception as e:
                logger.error(f"Error reading {data_type} {csv_path}: {str(e)}", exc_info=True)
    
    if dfs:
        logger.debug(f"Concatenating {len(dfs)} DataFrames")
        consolidated = pd.concat(dfs, ignore_index=True)
        consolidated = clean_consolidated_data(consolidated, data_type)
        
        if data_type == "indices":
            desired_columns = [
                'SEANCE', 'CODE_INDICE', 'LIB_INDICE', 'INDICE_JOUR',
                'INDICE_VEILLE', 'VARIATION_VEILLE', 'INDICE_PLUS_HAUT',
                'INDICE_PLUS_BAS', 'INDICE_OUV'
            ]
        else:
            desired_columns = [
                'SEANCE', 'GROUPE', 'CODE', 'VALEUR', 'OUVERTURE', 'CLOTURE',
                'PLUS_BAS', 'PLUS_HAUT', 'QUANTITE_NEGOCIEE', 
                'NB_TRANSACTION', 'CAPITAUX'
            ]
        
        available_columns = [col for col in desired_columns if col in consolidated.columns]
        consolidated = consolidated[available_columns]
        
        os.makedirs(os.path.join("..", "data", "input", data_type), exist_ok=True)
        consolidated_path = os.path.join("..", "data", "input", data_type, f"{data_type}.csv")
        consolidated.to_csv(consolidated_path, index=False)
        
        file_size = os.path.getsize(consolidated_path) / (1024 * 1024)
        logger.info(f"Created consolidated {data_type} file: {consolidated_path}")
        logger.info(f"Consolidated {data_type} file size: {file_size:.2f} MB")
        
        return file_size
    
    logger.warning("No data available to consolidate")
    return 0

def print_summary(start_time, successful_years, failed_years, data_types):
    """Print a professional summary of the execution"""
    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)
    
    summary = [
        "\n" + "="*80,
        "BVMT DATA PROCESSING SUMMARY (2008 - 2024)".center(80),
        "="*80,
        f"{'Total processing time:':<30} {int(minutes)} minutes {seconds:.2f} seconds",
        ""
    ]
    
    for data_type in data_types:
        summary.extend([
            f"{data_type.upper():<30} {'='*50}",
            f"{'Successful years:':<30} {len(successful_years[data_type]):<10} {sorted(successful_years[data_type])}",
            f"{'Failed years:':<30} {len(failed_years[data_type]):<10} {sorted(failed_years[data_type])}",
            f"{'Output file size:':<30} {data_types[data_type]:.2f} MB",
            ""
        ])
    
    summary.append("="*80)
    
    for line in summary:
        logger.info(line)

def main():
    """Main processing function with enhanced logging"""
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
    
    # Process indices first (with special handling for 2015)
    for year in range(start_year, end_year + 1):
        if process_year(year, "indices"):
            successful_years["indices"].append(year)
        else:
            failed_years["indices"].append(year)
    
    # Then process cotations
    for year in range(start_year, end_year + 1):
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
    logger.info("Starting BVMT data processing")
    print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))
    print("winutils exists:", os.path.exists(os.path.join(os.environ.get('HADOOP_HOME', ''), 'bin', 'winutils.exe')))
    try:
        successful_years, failed_years = main()
        logger.info("BVMT data processing completed successfully")
    except Exception as e:
        logger.critical(f"Fatal error in main execution: {str(e)}", exc_info=True)
        raise
    
    