"""
AWS configuration management
"""
import os
import boto3
from typing import Dict, Any


class AWSConfig:
    """AWS configuration manager"""
    
    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'dev')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.account_id = os.getenv('AWS_ACCOUNT_ID')
        
        # S3 configuration
        self.s3_config = {
            'raw_bucket': f'ecommerce-raw-data-{self.environment}',
            'processed_bucket': f'ecommerce-processed-data-{self.environment}',
            'failed_bucket': f'ecommerce-failed-data-{self.environment}'
        }
        
        # DynamoDB configuration
        self.dynamodb_config = {
            'category_table': f'CategoryDailyKPIs-{self.environment}',
            'daily_table': f'DailyOrderKPIs-{self.environment}'
        }
        
        # ECS configuration
        self.ecs_config = {
            'cluster_name': f'ecommerce-pipeline-{self.environment}',
            'task_definition': f'validation-task-{self.environment}'
        }
    
    def get_s3_client(self):
        """Get S3 client"""
        return boto3.client('s3', region_name=self.region)
    
    def get_dynamodb_client(self):
        """Get DynamoDB client"""
        return boto3.client('dynamodb', region_name=self.region)
    
    def get_dynamodb_resource(self):
        """Get DynamoDB resource"""
        return boto3.resource('dynamodb', region_name=self.region)