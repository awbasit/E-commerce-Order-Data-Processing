import boto3
import re
from datetime import datetime
from urllib.parse import urlparse
import sys
from helper_functions.helper_func import init_spark, read_csv_from_s3, validate_and_enrich, log

# === CONFIGURATION ===
INPUT_PREFIX= "s3://project6dt/raw_data"
BAD_ROW_PATH = "s3://project6dt/bad_rows"
LOG_PATH = "s3://project6dt/logs/validation"
CLOUDWATCH_GROUP = "ecommerce-pipeline-log-group"
CLOUDWATCH_STREAM = "validation-stream"
DATASET = "order_items"


# === S3 Client ===
s3_client = boto3.client("s3")

def list_csv_files(s3_prefix):
    """List all CSV files recursively in a given S3 prefix"""
    parsed = urlparse(s3_prefix)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    file_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                file_keys.append(f"s3://{bucket}/{key}")
    
    return file_keys

def infer_dataset_from_path(path):
    """Infer dataset name from file path"""
    if "order_items" in path:
        return "order_items"
    elif "orders" in path:
        return "orders"
    elif "products" in path:
        return "products"
    else:
        return None

def main():
    try:
        spark = init_spark("ECS-Validation-Dynamic")

        # Get all CSV files from the input prefix
        all_csv_files = list_csv_files(INPUT_PREFIX)
        if not all_csv_files:
            log(" No CSV files found in input directory", log_file_path=LOG_PATH, spark=spark)
            return

        for file_path in all_csv_files:
            dataset = infer_dataset_from_path(file_path)
            if not dataset:
                log(f" Skipping file (couldn't infer dataset): {file_path}", log_file_path=LOG_PATH, spark=spark)
                continue

            try:
                log(f" Validating file: {file_path}", log_file_path=LOG_PATH, spark=spark)

                df = read_csv_from_s3(spark, file_path, sample_only=True)
                validate_and_enrich(
                    df, dataset,
                    bad_row_path=BAD_ROW_PATH,
                    file_path=file_path,
                    spark=spark
                )

                log(f" Validation passed for: {file_path}", log_file_path=LOG_PATH, spark=spark)
            except Exception as e:
                log(f" Validation failed for {file_path}: {e}", log_file_path=LOG_PATH, spark=spark)

    except Exception as e:
        log(f" Entire validation job failed: {e}", log_file_path=LOG_PATH)

if __name__ == "__main__":
    main()