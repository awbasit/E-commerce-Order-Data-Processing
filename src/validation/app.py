# src/validation/app.py
"""
Main validation service entry point
"""
import os
import sys
import json
import traceback
from datetime import datetime
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, '/app/src')

from common.logging_config import setup_logging
from common.error_handlers import ValidationError, handle_exception
from common.s3_utils import S3Utils
from config.aws_config import AWSConfig
from validation.schema_validator import SchemaValidator
from validation.business_rules import BusinessRulesValidator
from validation.data_quality import DataQualityValidator


class ValidationService:
    """Main validation service orchestrator"""
    
    def __init__(self):
        self.logger = setup_logging("validation-service")
        self.aws_config = AWSConfig()
        self.s3_utils = S3Utils()
        
        # Initialize validators
        self.schema_validator = SchemaValidator()
        self.business_validator = BusinessRulesValidator()
        self.quality_validator = DataQualityValidator()
        
        self.logger.info("Validation service initialized", 
                        environment=os.getenv('ENVIRONMENT', 'dev'))
    
    def validate_file(self, s3_bucket: str, s3_key: str) -> Dict[str, Any]:
        """
        Validate a CSV file from S3
        
        Args:
            s3_bucket: S3 bucket name
            s3_key: S3 object key
            
        Returns:
            Dict containing validation results
        """
        start_time = datetime.now()
        
        try:
            self.logger.info("Starting file validation", 
                           bucket=s3_bucket, key=s3_key)
            
            # Get file info
            file_info = self.s3_utils.get_file_info(s3_bucket, s3_key)
            
            # Validate file in chunks
            validation_results = []
            total_records = 0
            
            for chunk_num, chunk_df in enumerate(self.s3_utils.stream_csv_chunks(s3_bucket, s3_key)):
                chunk_result = self._validate_chunk(chunk_df, chunk_num)
                validation_results.append(chunk_result)
                total_records += len(chunk_df)
                
                # Stop if too many errors
                total_errors = sum(len(result.get('errors', [])) for result in validation_results)
                if total_errors > 100:  # Configurable threshold
                    self.logger.warning("Too many validation errors, stopping", 
                                      total_errors=total_errors)
                    break
            
            # Aggregate results
            aggregated_result = self._aggregate_results(validation_results)
            
            # Calculate final metrics
            processing_time = (datetime.now() - start_time).total_seconds()
            success = len(aggregated_result['errors']) == 0
            
            result = {
                'success': success,
                'file_info': {
                    'bucket': s3_bucket,
                    'key': s3_key,
                    'size': file_info['size'],
                    'record_count': total_records
                },
                'validation_summary': aggregated_result,
                'processing_time': processing_time,
                'timestamp': datetime.now().isoformat()
            }
            
            if success:
                self.logger.info("File validation successful", 
                               bucket=s3_bucket, key=s3_key,
                               processing_time=processing_time,
                               record_count=total_records)
            else:
                self.logger.error("File validation failed", 
                                bucket=s3_bucket, key=s3_key,
                                error_count=len(aggregated_result['errors']))
                
                # Move failed file to quarantine
                self._quarantine_failed_file(s3_bucket, s3_key, result)
            
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
    
    def _validate_chunk(self, chunk_df, chunk_num: int) -> Dict[str, Any]:
        """Validate a single chunk of data"""
        chunk_result = {
            'chunk_num': chunk_num,
            'record_count': len(chunk_df),
            'errors': [],
            'warnings': []
        }
        
        try:
            # Schema validation
            schema_results = self.schema_validator.validate_dataframe(chunk_df)
            chunk_result['errors'].extend(schema_results['errors'])
            chunk_result['warnings'].extend(schema_results['warnings'])
            
            # Business rules validation
            business_results = self.business_validator.validate_dataframe(chunk_df)
            chunk_result['errors'].extend(business_results['errors'])
            chunk_result['warnings'].extend(business_results['warnings'])
            
            # Data quality validation
            quality_results = self.quality_validator.validate_dataframe(chunk_df)
            chunk_result['errors'].extend(quality_results['errors'])
            chunk_result['warnings'].extend(quality_results['warnings'])
            
        except Exception as e:
            chunk_result['errors'].append({
                'type': 'CHUNK_PROCESSING_ERROR',
                'message': f"Chunk {chunk_num} processing failed: {str(e)}",
                'chunk_num': chunk_num
            })
        
        return chunk_result
    
    def _aggregate_results(self, validation_results: list) -> Dict[str, Any]:
        """Aggregate validation results from all chunks"""
        aggregated = {
            'total_chunks': len(validation_results),
            'total_records': sum(r.get('record_count', 0) for r in validation_results),
            'errors': [],
            'warnings': []
        }
        
        for result in validation_results:
            aggregated['errors'].extend(result.get('errors', []))
            aggregated['warnings'].extend(result.get('warnings', []))
        
        return aggregated
    
    def _quarantine_failed_file(self, bucket: str, key: str, validation_result: Dict[str, Any]):
        """Move failed file to quarantine bucket"""
        try:
            failed_bucket = f"ecommerce-failed-data-{os.getenv('ENVIRONMENT', 'dev')}"
            self.s3_utils.copy_file(bucket, key, failed_bucket, key)
            
            # Save validation report
            report_key = f"{key}.validation_report.json"
            self.s3_utils.put_object(failed_bucket, report_key, 
                                   json.dumps(validation_result, indent=2))
            
            self.logger.info("Failed file quarantined", 
                           original_bucket=bucket, 
                           failed_bucket=failed_bucket, 
                           key=key)
            
        except Exception as e:
            self.logger.error("Failed to quarantine file", 
                            bucket=bucket, key=key, error=str(e))

def main():
    """Main entry point"""
    try:
        # Get input from environment or command line
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
        
        # Extract S3 details
        s3_bucket = event.get('s3_bucket') or event.get('Records', [{}])[0].get('s3', {}).get('bucket', {}).get('name')
        s3_key = event.get('s3_key') or event.get('Records', [{}])[0].get('s3', {}).get('object', {}).get('key')
        
        if not s3_bucket or not s3_key:
            raise ValidationError("Missing S3 bucket or key in event")
        
        # Initialize service and validate
        service = ValidationService()
        result = service.validate_file(s3_bucket, s3_key)
        
        # Output result for Step Functions
        output = {
            'statusCode': 200 if result['success'] else 400,
            'body': result,
            'next_step': 'transformation' if result['success'] else 'error_handling'
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