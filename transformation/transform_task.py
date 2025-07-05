"""
Main transformation service entry point
"""
import os
import sys
import json
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, '/app/src')

from common.logging_config import setup_logging
from common.error_handlers import handle_exception
from common.s3_utils import S3Utils
from config.aws_config import AWSConfig
from transformation.kpi_calculator import KPICalculator
from transformation.aggregators import DataAggregator
from transformation.dynamo_writer import DynamoDBWriter


class TransformationService:
    """Main transformation service orchestrator"""
    
    def __init__(self):
        self.logger = setup_logging("transformation-service")
        self.aws_config = AWSConfig()
        self.s3_utils = S3Utils()
        
        # Initialize components
        self.kpi_calculator = KPICalculator()
        self.aggregator = DataAggregator()
        self.dynamo_writer = DynamoDBWriter()
        
        self.logger.info("Transformation service initialized",
                        environment=os.getenv('ENVIRONMENT', 'dev'))
    
    def transform_file(self, s3_bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        Transform a validated CSV file
        
        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            Dict containing transformation results
        """
        start_time = datetime.now()
        
        try:
            self.logger.info("Starting file transformation",
                           bucket=s3_bucket, key=s3_key)
            
            # Get file info
            file_info = self.s3_utils.get_file_info(s3_bucket, s3_key)
            
            # Process file in chunks and calculate KPIs
            category_kpis = {}
            daily_kpis = {}
            total_records = 0
            
            for chunk_num, chunk_df in enumerate(self.s3_utils.stream_csv_chunks(s3_bucket, s3_key)):
                chunk_result = self._process_chunk(chunk_df, chunk_num)
                
                # Aggregate KPIs
                category_kpis = self.aggregator.merge_category_kpis(category_kpis, chunk_result['category_kpis'])
                daily_kpis = self.aggregator.merge_daily_kpis(daily_kpis, chunk_result['daily_kpis'])
                
                total_records += len(chunk_df)
                
                self.logger.info(f"Processed chunk {chunk_num}",
                               chunk_records=len(chunk_df),
                               total_records=total_records)
            
            # Write KPIs to DynamoDB
            self.dynamo_writer.write_category_kpis(category_kpis)
            self.dynamo_writer.write_daily_kpis(daily_kpis)
            
            # Move processed file to archive
            self._archive_processed_file(s3_bucket, s3_key)
            
            # Calculate final metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                'success': True,
                'file_info': {
                    'bucket': s3_bucket,
                    'key': s3_key,
                    'size': file_info['size'],
                    'record_count': total_records
                },
                'kpi_summary': {
                    'category_kpis_count': len(category_kpis),
                    'daily_kpis_count': len(daily_kpis),
                    'categories_processed': list(category_kpis.keys())
                },
                'processing_time': processing_time,
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info("File transformation successful",
                           bucket=s3_bucket, key=s3_key,
                           processing_time=processing_time,
                           record_count=total_records,
                           category_kpis=len(category_kpis))
            
            return result
            
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            error_details = handle_exception(e, self.logger)
            
            return {
                'success': False,
                'file_info': {'bucket': s3_bucket, 'key': s3_key},
                'error': error_details,
                'processing_time': processing_time,
                'timestamp': datetime.now().isoformat()
            }
    
    def _process_chunk(self, chunk_df, chunk_num: int) -> Dict[str, Any]:
        """Process a single chunk of data"""
        try:
            # Calculate KPIs for this chunk
            category_kpis = self.kpi_calculator.calculate_category_kpis(chunk_df)
            daily_kpis = self.kpi_calculator.calculate_daily_kpis(chunk_df)
            
            return {
                'chunk_num': chunk_num,
                'category_kpis': category_kpis,
                'daily_kpis': daily_kpis,
                'record_count': len(chunk_df)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process chunk {chunk_num}",
                            error=str(e))
            raise
    
    def _archive_processed_file(self, bucket: str, key: str):
        """Move processed file to archive bucket"""
        try:
            archive_bucket = self.aws_config.s3_config['processed_bucket']
            self.s3_utils.copy_file(bucket, key, archive_bucket, key)
            
            self.logger.info("File archived successfully",
                           original_bucket=bucket,
                           archive_bucket=archive_bucket,
                           key=key)
            
        except Exception as e:
            self.logger.error("Failed to archive file",
                            bucket=bucket, key=key, error=str(e))
            # Don't fail the whole process if archival fails
            pass


def main():
    """Main entry point"""
    try:
        # Get input from environment
        event_json = os.getenv('STEP_FUNCTION_INPUT')
        
        if not event_json:
            if len(sys.argv) >= 3:
                event_json = json.dumps({
                    's3_bucket': sys.argv[1],
                    's3_key': sys.argv[2]
                })
            else:
                raise ValueError("No input provided")
        
        event = json.loads(event_json)
        
        # Extract S3 details from validation output
        if 'body' in event and 'file_info' in event['body']:
            file_info = event['body']['file_info']
            s3_bucket = file_info['bucket']
            s3_key = file_info['key']
        else:
            s3_bucket = event.get('s3_bucket')
            s3_key = event.get('s3_key')
        
        if not s3_bucket or not s3_key:
            raise ValueError("Missing S3 bucket or key in event")
        
        # Initialize service and transform
        service = TransformationService()
        result = service.transform_file(s3_bucket, s3_key)
        
        # Output result for Step Functions
        output = {
            'statusCode': 200 if result['success'] else 500,
            'body': result,
            'next_step': 'complete' if result['success'] else 'error_handling'
        }
        
        print(json.dumps(output, indent=2))
        sys.exit(0 if result['success'] else 1)
        
    except Exception as e:
        error_result = {
            'statusCode': 500,
            'body': {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()