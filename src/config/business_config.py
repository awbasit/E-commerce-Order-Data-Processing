"""
Business configuration and rules
"""
import os
from typing import List, Dict, Any
from datetime import datetime, timedelta


class BusinessConfig:
    """Business rules and configuration"""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'dev')
        
        # Valid business values
        self.valid_categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden',
            'Sports & Outdoors', 'Beauty', 'Toys', 'Automotive',
            'Health & Personal Care', 'Pet Supplies'
        ]
        
        self.valid_statuses = ['completed', 'cancelled', 'returned']
        
        # Business limits
        self.limits = {
            'max_order_amount': 10000.00,
            'min_order_amount': 0.01,
            'max_quantity': 100,
            'max_file_size_mb': 500,
            'max_processing_time_minutes': 60
        }
        
        # Date ranges
        self.date_ranges = {
            'min_order_date': datetime.now() - timedelta(days=365),
            'max_order_date': datetime.now() + timedelta(days=1)
        }
        
        # Validation thresholds
        self.validation_thresholds = {
            'max_error_rate': 0.05,  # 5% max error rate
            'max_duplicate_rate': 0.02,  # 2% max duplicate rate
            'max_missing_data_rate': 0.10  # 10% max missing data
        }
    
    def get_category_config(self) -> Dict[str, Any]:
        """Get category-specific configuration"""
        return {
            'Electronics': {
                'min_price': 1.00,
                'max_price': 5000.00,
                'typical_quantity_range': (1, 5)
            },
            'Clothing': {
                'min_price': 5.00,
                'max_price': 500.00,
                'typical_quantity_range': (1, 10)
            },
            'Books': {
                'min_price': 0.99,
                'max_price': 200.00,
                'typical_quantity_range': (1, 20)
            }
        }
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return {
            'chunk_size': int(os.getenv('CHUNK_SIZE', '10000')),
            'max_workers': int(os.getenv('MAX_WORKERS', '4')),
            'timeout_seconds': int(os.getenv('TIMEOUT_SECONDS', '3600')),
            'retry_attempts': int(os.getenv('RETRY_ATTEMPTS', '3'))
        }