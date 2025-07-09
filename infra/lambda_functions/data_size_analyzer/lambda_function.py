#data_size_analyzer (lambda function)
import boto3
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal

def lambda_handler(event, context):
    """
    Simple file monitor that triggers Step Function based on file count or time
    """
    
    # Configuration
    BUCKET_NAME = os.environ.get('DATA_BUCKET')
    FILE_THRESHOLD = int(os.environ.get('FILE_THRESHOLD', '5'))
    TIME_THRESHOLD_MINUTES = int(os.environ.get('TIME_THRESHOLD_MINUTES', '5'))
    MAX_BATCH_SIZE = int(os.environ.get('MAX_BATCH_SIZE', '10'))  # New batch size limit
    STEP_FUNCTION_ARN = os.environ.get('STEP_FUNCTION_ARN')
    MONITORING_TABLE = os.environ.get('MONITORING_TABLE', 'FileProcessingMonitor')
    DATA_PREFIX = os.environ.get('DATA_PREFIX', 'raw_data/')
    
    # Initialize clients
    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    stepfunctions = boto3.client('stepfunctions')
    
    try:
        monitor_table = dynamodb.Table(MONITORING_TABLE)
        
        # Determine if this is S3 event or scheduled check
        if 'Records' in event:
            # S3 event - new file uploaded
            return handle_new_file(event, s3_client, monitor_table, stepfunctions, 
                                 BUCKET_NAME, FILE_THRESHOLD, TIME_THRESHOLD_MINUTES, MAX_BATCH_SIZE,
                                 STEP_FUNCTION_ARN, DATA_PREFIX)
        else:
            # Scheduled check - time-based trigger
            return handle_time_check(s3_client, monitor_table, stepfunctions,
                                   BUCKET_NAME, FILE_THRESHOLD, TIME_THRESHOLD_MINUTES, MAX_BATCH_SIZE,
                                   STEP_FUNCTION_ARN, DATA_PREFIX)
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}

def handle_new_file(event, s3_client, monitor_table, stepfunctions, bucket_name, 
                   file_threshold, time_threshold_minutes, max_batch_size, step_function_arn, data_prefix):
    """Handle new file upload"""
    
    print("New file uploaded")
    
    # Get monitoring state
    state = get_monitoring_state(monitor_table)
    
    # Check if processing is running
    if is_processing_running(stepfunctions, state):
        print("Processing already running - file will be picked up in next batch")
        return {'statusCode': 200, 'body': 'Processing running, file queued'}
    
    # Count current files in bucket (excluding processed ones)
    file_count, new_files = count_files_in_bucket(s3_client, bucket_name, data_prefix, state.get('processed_files', set()))
    
    # Check if we should trigger
    should_trigger, reason = should_trigger_processing(state, file_count, file_threshold, time_threshold_minutes)
    
    if should_trigger:
        return trigger_step_function_with_batching(stepfunctions, step_function_arn, monitor_table, 
                                   bucket_name, data_prefix, new_files, file_count, max_batch_size, reason)
    else:
        # Update state with current time if this is first file
        if not state.get('first_file_time'):
            update_monitoring_state(monitor_table, {
                'first_file_time': datetime.utcnow().isoformat(),
                'current_execution': None
            })
        
        print(f"Waiting for more files: {file_count}/{file_threshold}")
        return {'statusCode': 200, 'body': f'Waiting: {file_count}/{file_threshold} files'}

def handle_time_check(s3_client, monitor_table, stepfunctions, bucket_name,
                     file_threshold, time_threshold_minutes, max_batch_size, step_function_arn, data_prefix):
    """Handle scheduled time check"""
    
    print("Scheduled time check")
    
    # Get monitoring state
    state = get_monitoring_state(monitor_table)
    
    # Check if processing is running
    if is_processing_running(stepfunctions, state):
        print("Processing already running")
        return {'statusCode': 200, 'body': 'Processing running'}
    
    # Count current files (excluding processed ones)
    file_count, new_files = count_files_in_bucket(s3_client, bucket_name, data_prefix, state.get('processed_files', set()))
    
    if file_count == 0:
        print("No files to process")
        return {'statusCode': 200, 'body': 'No files'}
    
    # Check if we should trigger based on time
    should_trigger, reason = should_trigger_processing(state, file_count, file_threshold, time_threshold_minutes)
    
    if should_trigger:
        return trigger_step_function_with_batching(stepfunctions, step_function_arn, monitor_table,
                                   bucket_name, data_prefix, new_files, file_count, max_batch_size, reason)
    else:
        print(f"Not ready to trigger: {reason}")
        return {'statusCode': 200, 'body': reason}

def get_monitoring_state(monitor_table):
    """Get current monitoring state from DynamoDB"""
    
    try:
        response = monitor_table.get_item(Key={'monitor_id': 'file_processing'})
        
        if 'Item' in response:
            return {
                'first_file_time': response['Item'].get('first_file_time'),
                'current_execution': response['Item'].get('current_execution'),
                'last_updated': response['Item'].get('last_updated'),
                'processed_files': set(response['Item'].get('processed_files', []))  # Track processed files
            }
        else:
            return {
                'first_file_time': None,
                'current_execution': None,
                'last_updated': datetime.utcnow().isoformat(),
                'processed_files': set()  # Empty set for new state
            }
            
    except Exception as e:
        print(f"Error getting monitoring state: {str(e)}")
        return {
            'first_file_time': None,
            'current_execution': None,
            'last_updated': datetime.utcnow().isoformat(),
            'processed_files': set()
        }

def is_processing_running(stepfunctions, state):
    """Check if Step Function is currently running"""
    
    current_execution = state.get('current_execution')
    if not current_execution:
        return False
    
    try:
        response = stepfunctions.describe_execution(executionArn=current_execution)
        status = response['status']
        print(f"Current execution {current_execution} status: {status}")
        return status in ['RUNNING']
    except Exception as e:
        print(f"Error checking execution status: {str(e)}")
        # If we can't check, assume it's still running to be safe
        return True

def count_files_in_bucket(s3_client, bucket_name, data_prefix, processed_files=None):
    """Count NEW files in S3 bucket (excluding already processed ones)"""
    
    if processed_files is None:
        processed_files = set()
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=data_prefix)
        
        if 'Contents' not in response:
            return 0, []
        
        # Get current files and filter out processed ones
        new_files = []
        for obj in response['Contents']:
            if not obj['Key'].endswith('/') and obj['Size'] > 0:
                file_key = obj['Key']
                if file_key not in processed_files:
                    new_files.append({
                        'key': file_key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat()
                    })
                
        return len(new_files), new_files
        
    except Exception as e:
        print(f"Error counting files: {str(e)}")
        return 0, []

def should_trigger_processing(state, file_count, file_threshold, time_threshold_minutes):
    """Determine if processing should be triggered"""
    
    # Check file count threshold
    if file_count >= file_threshold:
        return True, f"File threshold reached: {file_count}/{file_threshold}"
    
    # Check time threshold
    first_file_time = state.get('first_file_time')
    if first_file_time and file_count > 0:
        first_time = datetime.fromisoformat(first_file_time.replace('Z', '+00:00'))
        current_time = datetime.utcnow().replace(tzinfo=first_time.tzinfo)
        minutes_passed = (current_time - first_time).total_seconds() / 60  # Convert to minutes
        
        if minutes_passed >= time_threshold_minutes:
            return True, f"Time threshold exceeded: {minutes_passed:.1f}/{time_threshold_minutes} minutes"
    
    return False, f"Waiting: {file_count}/{file_threshold} files"

def trigger_step_function_with_batching(stepfunctions, step_function_arn, monitor_table, 
                         bucket_name, data_prefix, new_files, file_count, max_batch_size, reason):
    """Trigger the Step Function with batch size limiting"""
    
    # Limit batch size to prevent overwhelming the system
    if len(new_files) > max_batch_size:
        files_to_process = new_files[:max_batch_size]
        remaining_files = new_files[max_batch_size:]
        
        print(f"Large batch detected: {len(new_files)} files")
        print(f"Processing first {len(files_to_process)} files, {len(remaining_files)} remaining")
        
        # Add note to reason about batching
        batch_reason = f"{reason} (Batch 1 of {(len(new_files) + max_batch_size - 1) // max_batch_size}: {len(files_to_process)} files)"
    else:
        files_to_process = new_files
        remaining_files = []
        batch_reason = reason
    
    execution_input = {
        'bucket': bucket_name,
        'prefix': data_prefix,
        'files': files_to_process,
        'fileCount': len(files_to_process),
        'totalAvailableFiles': len(new_files),
        'remainingFiles': len(remaining_files),
        'maxBatchSize': max_batch_size,
        'triggerReason': batch_reason,
        'triggerTime': datetime.utcnow().isoformat()
    }
    
    try:
        execution_name = f"processing-{int(datetime.utcnow().timestamp())}"
        
        response = stepfunctions.start_execution(
            stateMachineArn=step_function_arn,
            name=execution_name,
            input=json.dumps(execution_input)
        )
        
        execution_arn = response['executionArn']
        
        # Mark only the files being processed (not the remaining ones)
        processed_file_keys = [f['key'] for f in files_to_process]
        
        # Update monitoring state
        update_monitoring_state(monitor_table, {
            'current_execution': execution_arn,
            'first_file_time': None,  # Reset timer
            'last_updated': datetime.utcnow().isoformat(),
            'processed_files': processed_file_keys,
            'remaining_batch_files': len(remaining_files)  # Track remaining files
        })
        
        print(f"Triggered Step Function: {execution_arn}")
        print(f"Processing {len(files_to_process)} files: {[f['key'] for f in files_to_process]}")
        print(f"Batch reason: {batch_reason}")
        
        if remaining_files:
            print(f"Remaining {len(remaining_files)} files will be processed in next batch")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'action': 'triggered',
                'executionArn': execution_arn,
                'filesInBatch': len(files_to_process),
                'totalFiles': len(new_files),
                'remainingFiles': len(remaining_files),
                'processedFiles': [f['key'] for f in files_to_process],
                'reason': batch_reason
            })
        }
        
    except Exception as e:
        print(f"Error triggering Step Function: {str(e)}")
        return {'statusCode': 500, 'body': f'Failed to trigger: {str(e)}'}

def update_monitoring_state(monitor_table, state):
    """Update monitoring state"""
    
    try:
        item = {
            'monitor_id': 'file_processing',
            'current_execution': state.get('current_execution'),
            'first_file_time': state.get('first_file_time'),
            'last_updated': state.get('last_updated', datetime.utcnow().isoformat()),
            'processed_files': list(state.get('processed_files', [])),  # Convert set to list for DynamoDB
            'remaining_batch_files': state.get('remaining_batch_files', 0)  # Track remaining files
        }
        
        monitor_table.put_item(Item=item)
        
    except Exception as e:
        print(f"Error updating state: {str(e)}")