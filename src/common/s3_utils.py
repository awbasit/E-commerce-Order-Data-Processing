"""
S3 utility functions
"""
import boto3
import pandas as pd
import io
from typing import Dict, Any, Iterator
from botocore.exceptions import ClientError


class S3Utils:
    """S3 utility functions"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def get_file_info(self, bucket: str, key: str) -> Dict[str, Any]:
        """Get file information from S3"""
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'unknown')
            }
        except ClientError as e:
            raise Exception(f"Failed to get file info: {e}")
    
    def stream_csv_chunks(self, bucket: str, key: str, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
        """Stream CSV file in chunks"""
        try:
            # Get the S3 object
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            
            # Read CSV in chunks
            with pd.read_csv(
                io.BytesIO(response['Body'].read()),
                chunksize=chunk_size,
                dtype=str  # Read all as strings initially
            ) as reader:
                for chunk in reader:
                    yield chunk
                    
        except Exception as e:
            raise Exception(f"Failed to stream CSV: {e}")
    
    def copy_file(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str):
        """Copy file between S3 buckets"""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        except ClientError as e:
            raise Exception(f"Failed to copy file: {e}")
    
    def put_object(self, bucket: str, key: str, content: str):
        """Put object to S3"""
        try:
            self.s3_client.put_object(Bucket=bucket, Key=key, Body=content)
        except ClientError as e:
            raise Exception(f"Failed to put object: {e}")