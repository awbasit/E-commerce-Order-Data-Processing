"""
Data quality validation for e-commerce data
"""
import pandas as pd
from typing import Dict, Any, List, Set
from collections import defaultdict


class DataQualityValidator:
    """Validates data quality aspects"""
    
    def __init__(self):
        self.seen_order_ids: Set[str] = set()
        self.duplicate_threshold = 0.05  # 5% max duplicates
        self.missing_threshold = 0.10    # 10% max missing
        
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate data quality for DataFrame
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        results = {
            'errors': [],
            'warnings': []
        }
        
        # Check for duplicates
        duplicate_errors = self._check_duplicates(df)
        results['errors'].extend(duplicate_errors)
        
        # Check missing data
        missing_errors = self._check_missing_data(df)
        results['errors'].extend(missing_errors)
        
        # Check for outliers
        outlier_warnings = self._check_outliers(df)
        results['warnings'].extend(outlier_warnings)
        
        return results
    
    def _check_duplicates(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Check for duplicate records"""
        errors = []
        
        # Check duplicate order IDs
        if 'order_id' in df.columns:
            for index, row in df.iterrows():
                order_id = str(row['order_id'])
                if order_id in self.seen_order_ids:
                    errors.append({
                        'type': 'DUPLICATE_ORDER_ID',
                        'message': f"Duplicate order ID: {order_id}",
                        'row_index': index,
                        'column': 'order_id',
                        'value': order_id
                    })
                else:
                    self.seen_order_ids.add(order_id)
        
        # Check for duplicate rows
        if len(df) > 1:
            duplicate_mask = df.duplicated(keep=False)
            if duplicate_mask.any():
                for index in df[duplicate_mask].index:
                    errors.append({
                        'type': 'DUPLICATE_ROW',
                        'message': "Duplicate row detected",
                        'row_index': index
                    })
        
        return errors
    
    def _check_missing_data(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Check for missing data patterns"""
        errors = []
        
        # Check missing percentage per column
        for column in df.columns:
            missing_count = df[column].isna().sum()
            missing_percentage = missing_count / len(df) if len(df) > 0 else 0
            
            if missing_percentage > self.missing_threshold:
                errors.append({
                    'type': 'HIGH_MISSING_DATA',
                    'message': f"High missing data in column '{column}': {missing_percentage:.1%}",
                    'column': column,
                    'missing_count': missing_count,
                    'missing_percentage': missing_percentage
                })
        
        # Check for rows with too many missing values
        missing_per_row = df.isna().sum(axis=1)
        max_missing_per_row = len(df.columns) * 0.3  # 30% threshold
        
        for index, missing_count in missing_per_row.items():
            if missing_count > max_missing_per_row:
                errors.append({
                    'type': 'ROW_TOO_MANY_MISSING',
                    'message': f"Row has too many missing values: {missing_count}/{len(df.columns)}",
                    'row_index': index,
                    'missing_count': missing_count
                })
        
        return errors
    
    def _check_outliers(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Check for statistical outliers"""
        warnings = []
        
        numeric_columns = ['quantity', 'unit_price', 'total_amount']
        
        for column in numeric_columns:
            if column in df.columns:
                try:
                    numeric_series = pd.to_numeric(df[column], errors='coerce')
                    
                    # Calculate z-scores
                    mean_val = numeric_series.mean()
                    std_val = numeric_series.std()
                    
                    if std_val > 0:
                        z_scores = abs((numeric_series - mean_val) / std_val)
                        outlier_mask = z_scores > 3.0  # 3 standard deviations
                        
                        for index in df[outlier_mask].index:
                            if not pd.isna(numeric_series.iloc[index]):
                                warnings.append({
                                    'type': 'STATISTICAL_OUTLIER',
                                    'message': f"Potential outlier in {column}: {df.loc[index, column]}",
                                    'row_index': index,
                                    'column': column,
                                    'value': float(df.loc[index, column]),
                                    'z_score': float(z_scores.iloc[index])
                                })
                
                except Exception:
                    continue
        
        return warnings