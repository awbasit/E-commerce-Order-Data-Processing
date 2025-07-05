"""
Business rules validation for e-commerce data
"""
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timedelta


class BusinessRulesValidator:
    """Validates business rules for e-commerce order data"""
    
    def __init__(self):
        self.valid_categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden',
            'Sports & Outdoors', 'Beauty', 'Toys', 'Automotive'
        ]
        
        self.valid_statuses = ['completed', 'cancelled', 'returned']
        
        self.min_order_date = datetime.now() - timedelta(days=365)  # 1 year ago
        self.max_order_date = datetime.now()
        
        self.min_amount = 0.01
        self.max_amount = 10000.00
        
        self.max_quantity = 100
    
    def validate_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Validate DataFrame against business rules
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dict with validation results
        """
        results = {
            'errors': [],
            'warnings': []
        }
        
        # Validate each row
        for index, row in df.iterrows():
            row_errors = self._validate_row(row, index)
            results['errors'].extend(row_errors)
        
        return results
    
    def _validate_row(self, row: pd.Series, row_index: int) -> List[Dict[str, Any]]:
        """Validate business rules for a single row"""
        errors = []
        
        # Validate category
        if 'category' in row.index:
            category = str(row['category'])
            if category not in self.valid_categories:
                errors.append({
                    'type': 'INVALID_CATEGORY',
                    'message': f"Invalid category: {category}",
                    'row_index': row_index,
                    'column': 'category',
                    'value': category,
                    'valid_values': self.valid_categories
                })
        
        # Validate status
        if 'status' in row.index:
            status = str(row['status'])
            if status not in self.valid_statuses:
                errors.append({
                    'type': 'INVALID_STATUS',
                    'message': f"Invalid status: {status}",
                    'row_index': row_index,
                    'column': 'status',
                    'value': status,
                    'valid_values': self.valid_statuses
                })
        
        # Validate order date
        if 'order_date' in row.index:
            try:
                order_date = pd.to_datetime(row['order_date'])
                if order_date < self.min_order_date:
                    errors.append({
                        'type': 'ORDER_DATE_TOO_OLD',
                        'message': f"Order date too old: {order_date}",
                        'row_index': row_index,
                        'column': 'order_date',
                        'value': str(order_date),
                        'min_date': str(self.min_order_date)
                    })
                elif order_date > self.max_order_date:
                    errors.append({
                        'type': 'ORDER_DATE_FUTURE',
                        'message': f"Order date in future: {order_date}",
                        'row_index': row_index,
                        'column': 'order_date',
                        'value': str(order_date)
                    })
            except (ValueError, TypeError):
                errors.append({
                    'type': 'INVALID_ORDER_DATE',
                    'message': f"Invalid order date format: {row['order_date']}",
                    'row_index': row_index,
                    'column': 'order_date',
                    'value': str(row['order_date'])
                })
        
        # Validate amounts
        if all(col in row.index for col in ['quantity', 'unit_price', 'total_amount']):
            try:
                quantity = float(row['quantity'])
                unit_price = float(row['unit_price'])
                total_amount = float(row['total_amount'])
                
                # Check positive values
                if quantity <= 0:
                    errors.append({
                        'type': 'INVALID_QUANTITY',
                        'message': f"Quantity must be positive: {quantity}",
                        'row_index': row_index,
                        'column': 'quantity',
                        'value': quantity
                    })
                
                if unit_price <= 0:
                    errors.append({
                        'type': 'INVALID_UNIT_PRICE',
                        'message': f"Unit price must be positive: {unit_price}",
                        'row_index': row_index,
                        'column': 'unit_price',
                        'value': unit_price
                    })
                
                # Check calculation
                expected_total = quantity * unit_price
                if abs(total_amount - expected_total) > 0.01:
                    errors.append({
                        'type': 'AMOUNT_MISMATCH',
                        'message': f"Total amount mismatch: {total_amount} != {expected_total}",
                        'row_index': row_index,
                        'column': 'total_amount',
                        'value': total_amount,
                        'expected': expected_total
                    })
                
                # Check limits
                if total_amount > self.max_amount:
                    errors.append({
                        'type': 'AMOUNT_TOO_HIGH',
                        'message': f"Total amount too high: {total_amount}",
                        'row_index': row_index,
                        'column': 'total_amount',
                        'value': total_amount,
                        'max_amount': self.max_amount
                    })
                
                if quantity > self.max_quantity:
                    errors.append({
                        'type': 'QUANTITY_TOO_HIGH',
                        'message': f"Quantity too high: {quantity}",
                        'row_index': row_index,
                        'column': 'quantity',
                        'value': quantity,
                        'max_quantity': self.max_quantity
                    })
                
            except (ValueError, TypeError):
                errors.append({
                    'type': 'INVALID_NUMERIC_VALUE',
                    'message': "Invalid numeric values in amount fields",
                    'row_index': row_index,
                    'columns': ['quantity', 'unit_price', 'total_amount']
                })
        
        return errors
