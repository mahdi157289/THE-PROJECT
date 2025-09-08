"""
Pandas-compatible schema definitions for bronze layer tables
Replaces PySpark schemas while maintaining the same field structure and validation
"""

def create_bronze_schemas():
    """
    Define schemas for bronze layer tables with appropriate data types
    
    The schema definitions use string types for numeric fields that might
    contain formatting issues in the raw data. These will be converted
    to appropriate numeric types during preprocessing.
    
    Returns:
        tuple: (cotations_schema, indices_schema)
    """
    # Cotations Schema
    cotations_schema = {
        'fields': [
            # Date and identification fields
            {'name': 'seance', 'type': 'datetime64[ns]', 'nullable': True},
            {'name': 'groupe', 'type': 'object', 'nullable': True},
            {'name': 'code', 'type': 'object', 'nullable': True},
            {'name': 'valeur', 'type': 'object', 'nullable': True},
            
            # Numeric fields - kept as object (string) for initial loading
            # These will be converted during preprocessing
            {'name': 'ouverture', 'type': 'object', 'nullable': True},
            {'name': 'cloture', 'type': 'object', 'nullable': True},
            {'name': 'plus_bas', 'type': 'object', 'nullable': True},
            {'name': 'plus_haut', 'type': 'object', 'nullable': True},
            {'name': 'quantite_negociee', 'type': 'object', 'nullable': True},
            {'name': 'nb_transaction', 'type': 'object', 'nullable': True},
            {'name': 'capitaux', 'type': 'object', 'nullable': True},
            
            # Year field added during preprocessing
            {'name': 'annee', 'type': 'int64', 'nullable': True},
        ],
        'description': 'Bronze layer cotations schema with string types for initial loading'
    }

    # Indices Schema
    indices_schema = {
        'fields': [
            # Date and identification fields
            {'name': 'seance', 'type': 'datetime64[ns]', 'nullable': True},
            {'name': 'code_indice', 'type': 'object', 'nullable': True},
            {'name': 'lib_indice', 'type': 'object', 'nullable': True},
            
            # Numeric fields - kept as object (string) for initial loading
            # These will be converted during preprocessing
            {'name': 'indice_jour', 'type': 'object', 'nullable': True},
            {'name': 'indice_veille', 'type': 'object', 'nullable': True},
            {'name': 'variation_veille', 'type': 'object', 'nullable': True},
            {'name': 'indice_plus_haut', 'type': 'object', 'nullable': True},
            {'name': 'indice_plus_bas', 'type': 'object', 'nullable': True},
            {'name': 'indice_ouv', 'type': 'object', 'nullable': True},
            
            # Year field added during preprocessing
            {'name': 'annee', 'type': 'int64', 'nullable': True},
        ],
        'description': 'Bronze layer indices schema with string types for initial loading'
    }

    return cotations_schema, indices_schema

def create_silver_schemas():
    """
    Define schemas for silver layer tables with correct data types
    after data cleansing in the bronze layer
    
    Returns:
        tuple: (cotations_schema, indices_schema)
    """
    # Cotations Schema with proper numeric types
    cotations_schema = {
        'fields': [
            # Date and identification fields
            {'name': 'seance', 'type': 'datetime64[ns]', 'nullable': False},  # Not nullable in silver
            {'name': 'groupe', 'type': 'object', 'nullable': True},
            {'name': 'code', 'type': 'object', 'nullable': False},  # Not nullable in silver
            {'name': 'valeur', 'type': 'object', 'nullable': True},
            
            # Now using proper numeric types
            {'name': 'ouverture', 'type': 'float64', 'nullable': True},
            {'name': 'cloture', 'type': 'float64', 'nullable': True},
            {'name': 'plus_bas', 'type': 'float64', 'nullable': True},
            {'name': 'plus_haut', 'type': 'float64', 'nullable': True},
            {'name': 'quantite_negociee', 'type': 'float64', 'nullable': True},
            {'name': 'nb_transaction', 'type': 'int64', 'nullable': True},
            {'name': 'capitaux', 'type': 'float64', 'nullable': True},
            
            # Year field
            {'name': 'annee', 'type': 'int64', 'nullable': False},  # Not nullable in silver
            
            # Metadata fields
            {'name': 'ingestion_timestamp', 'type': 'datetime64[ns]', 'nullable': False},
            {'name': 'source_file', 'type': 'object', 'nullable': False},
        ],
        'description': 'Silver layer cotations schema with proper numeric types'
    }

    # Indices Schema with proper numeric types
    indices_schema = {
        'fields': [
            # Date and identification fields
            {'name': 'seance', 'type': 'datetime64[ns]', 'nullable': False},  # Not nullable in silver
            {'name': 'code_indice', 'type': 'object', 'nullable': False},  # Not nullable in silver
            {'name': 'lib_indice', 'type': 'object', 'nullable': True},
            
            # Now using proper numeric types
            {'name': 'indice_jour', 'type': 'float64', 'nullable': True},
            {'name': 'indice_veille', 'type': 'float64', 'nullable': True},
            {'name': 'variation_veille', 'type': 'float64', 'nullable': True},
            {'name': 'indice_plus_haut', 'type': 'float64', 'nullable': True},
            {'name': 'indice_plus_bas', 'type': 'float64', 'nullable': True},
            {'name': 'indice_ouv', 'type': 'float64', 'nullable': True},
            
            # Year field
            {'name': 'annee', 'type': 'int64', 'nullable': False},  # Not nullable in silver
            
            # Metadata fields
            {'name': 'ingestion_timestamp', 'type': 'datetime64[ns]', 'nullable': False},
            {'name': 'source_file', 'type': 'object', 'nullable': False},
        ],
        'description': 'Silver layer indices schema with proper numeric types'
    }

    return cotations_schema, indices_schema

def validate_dataframe_schema(df, schema, layer_name="Unknown"):
    """
    Validate a DataFrame against a schema definition
    
    Args:
        df: pandas DataFrame to validate
        schema: schema definition dictionary
        layer_name: name of the layer for logging
    
    Returns:
        dict: validation results with field-by-field analysis
    """
    validation_results = {
        'layer': layer_name,
        'total_fields': len(schema['fields']),
        'validated_fields': 0,
        'field_details': [],
        'overall_valid': True
    }
    
    for field in schema['fields']:
        field_name = field['name']
        expected_type = field['type']
        expected_nullable = field['nullable']
        
        field_result = {
            'name': field_name,
            'expected_type': expected_type,
            'expected_nullable': expected_nullable,
            'actual_type': str(df[field_name].dtype) if field_name in df.columns else 'MISSING',
            'actual_nullable': df[field_name].isna().any() if field_name in df.columns else None,
            'valid': True,
            'issues': []
        }
        
        # Check if field exists
        if field_name not in df.columns:
            field_result['valid'] = False
            field_result['issues'].append('Field missing from DataFrame')
            validation_results['overall_valid'] = False
        else:
            # Check type compatibility (simplified type checking)
            actual_type = str(df[field_name].dtype)
            
            # Type validation logic
            if expected_type == 'object' and actual_type == 'object':
                pass  # String/object types are compatible
            elif expected_type == 'datetime64[ns]' and 'datetime' in actual_type:
                pass  # Datetime types are compatible
            elif expected_type == 'int64' and 'int' in actual_type:
                pass  # Integer types are compatible
            elif expected_type == 'float64' and 'float' in actual_type:
                pass  # Float types are compatible
            else:
                field_result['valid'] = False
                field_result['issues'].append(f'Type mismatch: expected {expected_type}, got {actual_type}')
                validation_results['overall_valid'] = False
            
            # Check nullability
            if not expected_nullable and df[field_name].isna().any():
                field_result['valid'] = False
                field_result['issues'].append('Field contains null values but should not be nullable')
                validation_results['overall_valid'] = False
        
        validation_results['field_details'].append(field_result)
        if field_result['valid']:
            validation_results['validated_fields'] += 1
    
    return validation_results

def get_schema_field_names(schema):
    """
    Extract field names from a schema definition
    
    Args:
        schema: schema definition dictionary
    
    Returns:
        list: list of field names
    """
    return [field['name'] for field in schema['fields']]

def get_schema_field_types(schema):
    """
    Extract field types from a schema definition
    
    Args:
        schema: schema definition dictionary
    
    Returns:
        dict: mapping of field names to their expected types
    """
    return {field['name']: field['type'] for field in schema['fields']}