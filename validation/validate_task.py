import boto3
import pandas as pd
from urllib.parse import urlparse
from helper_functions.helper_func import read_csv_from_s3, validate_and_enrich, log

# === CONFIGURATION ===
INPUT_PREFIX = "s3://project6dt/raw_data"
BAD_ROW_PATH = "s3://project6dt/bad_rows"
CLOUDWATCH_GROUP = "ecommerce-pipeline-log-group"
CLOUDWATCH_STREAM = "validation-stream"

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
        all_csv_files = list_csv_files(INPUT_PREFIX)
        if not all_csv_files:
            log("No CSV files found in input directory", 
                cloudwatch_group=CLOUDWATCH_GROUP, 
                cloudwatch_stream=CLOUDWATCH_STREAM)
            return

        log(f"Found {len(all_csv_files)} CSV files to process", 
            cloudwatch_group=CLOUDWATCH_GROUP, 
            cloudwatch_stream=CLOUDWATCH_STREAM)

        for file_path in all_csv_files:
            dataset = infer_dataset_from_path(file_path)
            if not dataset:
                log(f"Skipping file (couldn't infer dataset): {file_path}", 
                    cloudwatch_group=CLOUDWATCH_GROUP, 
                    cloudwatch_stream=CLOUDWATCH_STREAM)
                continue

            try:
                log(f"Validating file: {file_path}", 
                    cloudwatch_group=CLOUDWATCH_GROUP, 
                    cloudwatch_stream=CLOUDWATCH_STREAM)

                df = read_csv_from_s3(file_path)

                # Build referential data if needed
                ref_data_paths = {}

                if dataset == "order_items":
                    ref_data_paths["products"] = "s3://project6dt/raw_data/products/products.csv"

                    # Dynamically find and merge all orders_part*.csv
                    orders_parts = [
                        f for f in all_csv_files
                        if f.startswith("s3://project6dt/raw_data/orders/orders_part")
                    ]
                    if not orders_parts:
                        raise FileNotFoundError("No orders_part*.csv found for referential check.")

                    log(f"Loading {len(orders_parts)} orders files for referential integrity check", 
                        cloudwatch_group=CLOUDWATCH_GROUP, 
                        cloudwatch_stream=CLOUDWATCH_STREAM)
                    
                    orders_dfs = []
                    for orders_file in orders_parts:
                        orders_df = read_csv_from_s3(orders_file)
                        orders_dfs.append(orders_df)
                    
                    ref_data_paths["orders"] = pd.concat(orders_dfs, ignore_index=True)
                    log(f"Combined orders data: {len(ref_data_paths['orders'])} total rows", 
                        cloudwatch_group=CLOUDWATCH_GROUP, 
                        cloudwatch_stream=CLOUDWATCH_STREAM)

                validate_and_enrich(
                    df,
                    dataset=dataset,
                    bad_row_path=BAD_ROW_PATH,
                    file_path=file_path,
                    ref_data_paths=ref_data_paths
                )

                log(f" Validation passed for: {file_path}", 
                    cloudwatch_group=CLOUDWATCH_GROUP, 
                    cloudwatch_stream=CLOUDWATCH_STREAM)

            except Exception as e:
                log(f" Validation failed for {file_path}: {e}", 
                    cloudwatch_group=CLOUDWATCH_GROUP, 
                    cloudwatch_stream=CLOUDWATCH_STREAM)
                # Continue processing other files even if one fails
                continue

        log(" Validation job completed", 
            cloudwatch_group=CLOUDWATCH_GROUP, 
            cloudwatch_stream=CLOUDWATCH_STREAM)

    except Exception as e:
        log(f" Entire validation job failed: {e}", 
            cloudwatch_group=CLOUDWATCH_GROUP, 
            cloudwatch_stream=CLOUDWATCH_STREAM)
        raise

if __name__ == "__main__":
    main()