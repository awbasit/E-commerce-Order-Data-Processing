"""
Schema validation for CSV data
"""
import pandas as pd
import re
from typing import Dict, Any, List
from datetime import datetime


class SchemaValidator:
    """Validates CSV data against expected schema"""
    
    def __init__(self):
        self.required_columns = [
            'order_id', 'customer_id', 'product_id', 'category',
            'order_date', 'quantity', 'unit_price', 'total_amount',
            'status', 'created_timestamp'
        ]
        
        self.column_types = {
            'order_id': str,
            'customer_id': str,
            'product_id': str,
            'category': str,
            'order_date': 'datetime',
            'quantity': int,
            'unit_price': float,
            'total_amount': float,
            'status': str,
            'created_timestamp': 'datetime'
        }
        
        self.column_patterns = {
            'order_id': r'^ORD-[A-Z0-9]{6,}$',
            'customer_id': r'^CUST-[A-Z0-9]{6,}$',
            'product_id': r'^PROD-[A-Z0-9]{6,}$'
        }
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame against schema
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        results = {
            'errors': [],
            'warnings': []
        }
        
        # Check required columns
        missing_columns = set(self.required_columns) - set(df.columns)
        if missing_columns:
            results['errors'].append({
                'type': 'MISSING_COLUMNS',
                'message': f"Missing required columns: {missing_columns}",
                'columns': list(missing_columns)
            })
            return results
        
        # Validate each row
        for index, row in df.iterrows():
            row_errors = self._validate_row(row, index)
            results['errors'].extend(row_errors)
        
        return results
    
    def _validate_row(self, row: pd.Series, row_index: int) -> List[Dict[str, Any]]:
        """Validate a single row"""
        errors = []
        
        for column in self.required_columns:
            value = row[column]
            
            # Check for missing values
            if pd.isna(value) or str(value).strip() == '':
                errors.append({
                    'type': 'MISSING_VALUE',
                    'message': f"Missing value in required column '{column}'",
                    'row_index': row_index,
                    'column': column
                })
                continue
            
            # Validate data type
            if not self._validate_type(value, column):
                errors.append({
                    'type': 'INVALID_TYPE',
                    'message': f"Invalid type for column '{column}': {type(value).__name__}",
                    'row_index': row_index,
                    'column': column,
                    'value': str(value)
                })
            
            # Validate patterns
            if column in self.column_patterns:
                if not re.match(self.column_patterns[column], str(value)):
                    errors.append({
                        'type': 'INVALID_FORMAT',
                        'message': f"Invalid format for column '{column}': {value}",
                        'row_index': row_index,
                        'column': column,
                        'value': str(value),
                        'expected_pattern': self.column_patterns[column]
                    })
        
        return errors
    
    def _validate_type(self, value: Any, column: str) -> bool:
        """Validate data type for a column"""
        expected_type = self.column_types.get(column)
        
        if expected_type is None:
            return True
        
        try:
            if expected_type == 'datetime':
                pd.to_datetime(value)
            elif expected_type == int:
                int(float(value))  # Handle string numbers
            elif expected_type == float:
                float(value)
            elif expected_type == str:
                str(value)
            
            return True
            
        except (ValueError, TypeError):
            return False