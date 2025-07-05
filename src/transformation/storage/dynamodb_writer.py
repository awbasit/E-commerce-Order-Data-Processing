"""
DynamoDB writer for KPI data
"""
import boto3
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta
import json
from decimal import Decimal
from botocore.exceptions import ClientError
import random


class DynamoDBWriter:
    """Write KPI data to DynamoDB tables"""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.environment = os.getenv('ENVIRONMENT', 'dev')
        
        # Table names
        self.category_table_name = f'CategoryDailyKPIs-{self.environment}'
        self.daily_table_name = f'DailyOrderKPIs-{self.environment}'
        
        # Get table references
        self.category_table = self.dynamodb.Table(self.category_table_name)
        self.daily_table = self.dynamodb.Table(self.daily_table_name)
    
    def write_category_kpis(self, category_kpis: Dict[str, Any]):
        """
        Write category KPIs to DynamoDB
        
        Args:
            category_kpis: Dictionary of category KPI data
        """
        try:
            # Prepare items for batch writing
            items = []
            for key, kpi_data in category_kpis.items():
                item = self._prepare_category_item(kpi_data)
                items.append(item)
            
            # Write in batches
            self._batch_write_items(self.category_table, items)
            
        except Exception as e:
            raise Exception(f"Failed to write category KPIs: {e}")
    
    def write_daily_kpis(self, daily_kpis: Dict[str, Any]):
        """
        Write daily KPIs to DynamoDB
        
        Args:
            daily_kpis: Dictionary of daily KPI data
        """
        try:
            # Prepare items for batch writing
            items = []
            for date_key, kpi_data in daily_kpis.items():
                item = self._prepare_daily_item(kpi_data)
                items.append(item)
            
            # Write in batches
            self._batch_write_items(self.daily_table, items)
            
        except Exception as e:
            raise Exception(f"Failed to write daily KPIs: {e}")
    
    def _prepare_category_item(self, kpi_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare category KPI item for DynamoDB"""
        # Calculate TTL (expire after 1 year)
        ttl = int((datetime.now() + timedelta(days=365)).timestamp())
        
        return {
            'category': kpi_data['category'],
            'order_date': kpi_data['order_date'],
            'total_orders': kpi_data['total_orders'],
            'daily_revenue': Decimal(str(kpi_data['daily_revenue'])),
            'avg_order_value': Decimal(str(kpi_data['avg_order_value'])),
            'avg_return_rate': Decimal(str(kpi_data['avg_return_rate'])),
            'total_items_sold': kpi_data['total_items_sold'],
            'unique_customers': kpi_data['unique_customers'],
            'avg_items_per_order': Decimal(str(kpi_data['avg_items_per_order'])),
            'last_updated': kpi_data['timestamp'],
            'ttl': ttl
        }
    
    def _prepare_daily_item(self, kpi_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare daily KPI item for DynamoDB"""
        # Calculate TTL (expire after 1 year)
        ttl = int((datetime.now() + timedelta(days=365)).timestamp())
        
        return {
            'order_date': kpi_data['order_date'],
            'total_orders': kpi_data['total_orders'],
            'total_revenue': Decimal(str(kpi_data['total_revenue'])),
            'total_items_sold': kpi_data['total_items_sold'],
            'return_rate': Decimal(str(kpi_data['return_rate'])),
            'unique_customers': kpi_data['unique_customers'],
            'avg_order_value': Decimal(str(kpi_data['avg_order_value'])),
            'avg_items_per_order': Decimal(str(kpi_data['avg_items_per_order'])),
            'category_breakdown': json.dumps(kpi_data.get('category_breakdown', {})),
            'last_updated': kpi_data['timestamp'],
            'ttl': ttl
        }
    
    def _batch_write_items(self, table, items: List[Dict[str, Any]], batch_size: int = 25):
        """
        Write items to DynamoDB in batches with retry logic
        
        Args:
            table: DynamoDB table resource
            items: List of items to write
            batch_size: Size of each batch (max 25 for DynamoDB)
        """
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            # Retry logic for batch writes
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    with table.batch_writer() as batch_writer:
                        for item in batch:
                            batch_writer.put_item(Item=item)
                    break  # Success, exit retry loop
                    
                except ClientError as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise e
                    
                    # Wait before retry (exponential backoff)
                    import time
                    wait_time = (2 ** retry_count) + (random.uniform(0, 1) if 'random' in globals() else 0.5)
                    time.sleep(wait_time)
                
                except Exception as e:
                    # Non-retryable error
                    raise e
