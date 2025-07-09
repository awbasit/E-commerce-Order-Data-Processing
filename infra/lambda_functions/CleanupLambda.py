# --- Part 1: Improved Cleanup Lambda ---

import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    stepfunctions = boto3.client('stepfunctions')

    MONITORING_TABLE = 'FileProcessingMonitor'
    STEP_FUNCTION_ARN = event.get('stepFunctionArn')
    EXECUTION_ARN = event.get('executionArn')

    try:
        monitor_table = dynamodb.Table(MONITORING_TABLE)

        # Check execution status first
        exec_response = stepfunctions.describe_execution(executionArn=EXECUTION_ARN)
        status = exec_response['status']

        if status not in ['SUCCEEDED', 'FAILED', 'TIMED_OUT', 'ABORTED']:
            print(f"Execution {EXECUTION_ARN} still in progress, skipping cleanup")
            return {'statusCode': 200, 'body': json.dumps('Execution still running, skipping cleanup')}

        # Fetch monitoring state
        response = monitor_table.get_item(Key={'monitor_id': 'file_processing'})

        if 'Item' in response:
            remaining_files = response['Item'].get('remaining_batch_files', 0)

            if remaining_files > 0:
                print(f"Batch completed, {remaining_files} files remaining")

                monitor_table.update_item(
                    Key={'monitor_id': 'file_processing'},
                    UpdateExpression='SET current_execution = :null, last_updated = :timestamp, first_file_time = :first_file_time',
                    ExpressionAttributeValues={
                        ':null': None,
                        ':timestamp': datetime.utcnow().isoformat(),
                        ':first_file_time': datetime.utcnow().isoformat()
                    }
                )
            else:
                print("All files processed, clearing full state")

                monitor_table.update_item(
                    Key={'monitor_id': 'file_processing'},
                    UpdateExpression='SET processed_files = :empty_list, current_execution = :null, last_updated = :timestamp, remaining_batch_files = :zero, first_file_time = :null',
                    ExpressionAttributeValues={
                        ':empty_list': [],
                        ':null': None,
                        ':timestamp': datetime.utcnow().isoformat(),
                        ':zero': 0
                    }
                )
        return {
            'statusCode': 200,
            'body': json.dumps('Cleanup completed')
        }

    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Cleanup failed: {str(e)}')
        }
