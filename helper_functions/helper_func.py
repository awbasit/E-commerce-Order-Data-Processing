import boto3
import sys
import os
import json
import traceback
import pandas as pd
from io import BytesIO
from datetime import datetime
from urllib.parse import urlparse
from decimal import Decimal



# ========================
# READ CSV FROM S3
# ========================
def read_csv_from_s3(spark, s3_path, sample_only=False):
    try:
        df = spark.read.option("header", True).csv(s3_path)
        if sample_only:
            log(f"Sampled 5 rows from: {s3_path}", spark=spark)
            return df.limit(5)
        log(f"Loaded full dataset from: {s3_path}", spark=spark)
        return df
    except Exception as e:
        log(f"Failed to read CSV from {s3_path}: {e}", spark=spark)
        raise


# ========================
# VALIDATE AND ENRICH
# ========================
# def validate_and_enrich(df, dataset, bad_row_path, file_path, spark=None, cloudwatch_group=None, cloudwatch_stream=None):
#     try:
#         required_columns = {
#             "order_items": ["order_id", "product_id", "user_id", "sale_price", "created_at", "status"],
#             "products": ["id", "sku", "cost", "category", "name"],
#             "orders": ["order_id", "user_id", "created_at", "status", "num_of_item"]
#         }

#         if dataset not in required_columns:
#             raise ValueError(f"Unknown dataset '{dataset}' passed to validation")

#         # Normalize column names
#         df = df.select([col(c).alias(c.lower()) for c in df.columns])
#         required_cols = [c.lower() for c in required_columns[dataset]]
#         df_cols = set(df.columns)

#         missing = list(set(required_cols) - df_cols)
#         if missing:
#             raise ValueError(f"Missing columns for dataset '{dataset}': {missing}")

#         original_count = df.count()
#         log(f"{dataset}: Original row count: {original_count}", spark=spark, cloudwatch_group=cloudwatch_group, cloudwatch_stream=cloudwatch_stream)

#         # Null filter
#         not_null_condition = None
#         for c in required_cols:
#             if not_null_condition is None:
#                 not_null_condition = col(c).isNotNull()
#             else:
#                 not_null_condition = not_null_condition & col(c).isNotNull()

#         valid_df = df.filter(not_null_condition)
#         valid_count = valid_df.count()
#         log(f"{dataset}: Rows after null filtering: {valid_count}", spark=spark, cloudwatch_group=cloudwatch_group, cloudwatch_stream=cloudwatch_stream)

#         # Deduplication
#         dedup_df = valid_df.dropDuplicates()
#         final_count = dedup_df.count()
#         log(f"{dataset}: Rows after deduplication: {final_count}", spark=spark, cloudwatch_group=cloudwatch_group, cloudwatch_stream=cloudwatch_stream)

#         # Rejected rows
#         rejected_df = df.subtract(dedup_df)
#         rejected_count = rejected_df.count()
#         if rejected_count > 0:
#             log(f"{dataset}: Writing {rejected_count} rejected rows to bad row folder", spark=spark, cloudwatch_group=cloudwatch_group, cloudwatch_stream=cloudwatch_stream)

#             ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#             parsed_path = urlparse(file_path)
#             filename = parsed_path.path.split("/")[-1].replace(".csv", "")
#             bad_row_s3_path = f"{bad_row_path}/{dataset}/{filename}_badrows_{ts}/"

#             rejected_df.write.option("header", True).mode("overwrite").csv(bad_row_s3_path)

#         return dedup_df

#     except Exception as e:
#         log(f"Error in validation for {dataset}: {str(e)}", spark=spark, cloudwatch_group=cloudwatch_group, cloudwatch_stream=cloudwatch_stream)
#         traceback.print_exc()
#         raise

def check_referential_integrity(df_main, df_ref, main_key, ref_key, ref_name):
    valid_keys = set(df_ref[ref_key].dropna().unique())
    mask = df_main[main_key].isin(valid_keys)
    invalid_rows = df_main[~mask]

    if not invalid_rows.empty:
        log(f"Found {len(invalid_rows)} rows in '{main_key}' not found in '{ref_name}.{ref_key}'")
        return df_main[mask], invalid_rows
    return df_main, pd.DataFrame()


def validate_and_enrich(df, dataset, bad_row_path, file_path, ref_data_paths={}):
    required_columns = {
        "order_items": ["order_id", "product_id", "user_id", "sale_price", "created_at", "status"],
        "products": ["id", "sku", "cost", "category", "name"],
        "orders": ["order_id", "user_id", "created_at", "status", "num_of_item"]
    }

    if dataset not in required_columns:
        raise ValueError(f"Unknown dataset: {dataset}")

    df.columns = [c.lower() for c in df.columns]
    required_cols = [c.lower() for c in required_columns[dataset]]

    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    original_len = len(df)
    df = df.dropna(subset=required_cols)
    after_null_len = len(df)

    df = df.drop_duplicates()
    after_dedup_len = len(df)

    log(f"{dataset}: Original={original_len}, After Nulls={after_null_len}, After Dedup={after_dedup_len}")

    # Referential integrity checks
    rejected_referential = pd.DataFrame()
    if dataset == "order_items":
        if "products" in ref_data_paths:
            df_products = read_csv_from_s3(ref_data_paths["products"])
            df, bad = check_referential_integrity(df, df_products, "product_id", "id", "products")
            rejected_referential = pd.concat([rejected_referential, bad])
        if "orders" in ref_data_paths:
            df_orders = read_csv_from_s3(ref_data_paths["orders"])
            df, bad = check_referential_integrity(df, df_orders, "order_id", "order_id", "orders")
            rejected_referential = pd.concat([rejected_referential, bad])

    # Save rejected rows
    if rejected_referential is not None and not rejected_referential.empty:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        parsed = urlparse(bad_row_path)
        filename = os.path.basename(file_path).replace(".csv", "")
        rejected_key = f"{parsed.path.strip('/')}/{dataset}/{filename}_badrows_{ts}.csv"

        buffer = BytesIO()
        rejected_referential.to_csv(buffer, index=False)
        buffer.seek(0)

        s3 = boto3.client("s3")
        s3.put_object(Bucket=parsed.netloc, Key=rejected_key, Body=buffer)
        log(f"Rejected rows saved to: s3://{parsed.netloc}/{rejected_key}")

    return df


# ========================
# LOGGING UTILITIES
# ========================
def log_to_console(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")


def log_to_cloudwatch(message, cloudwatch_group, cloudwatch_stream):
    try:
        logs_client = boto3.client("logs")
        ts_ms = int(datetime.utcnow().timestamp() * 1000)

        try:
            logs_client.create_log_group(logGroupName=cloudwatch_group)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        try:
            logs_client.create_log_stream(
                logGroupName=cloudwatch_group,
                logStreamName=cloudwatch_stream
            )
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        streams = logs_client.describe_log_streams(
            logGroupName=cloudwatch_group,
            logStreamNamePrefix=cloudwatch_stream,
            limit=1
        ).get("logStreams", [])

        token = streams[0].get("uploadSequenceToken") if streams else None

        log_event = {
            "logGroupName": cloudwatch_group,
            "logStreamName": cloudwatch_stream,
            "logEvents": [{
                "timestamp": ts_ms,
                "message": message
            }]
        }

        if token:
            log_event["sequenceToken"] = token

        logs_client.put_log_events(**log_event)

    except Exception as e:
        print(f"CloudWatch logging error: {e}")
        traceback.print_exc()


def log_to_s3(message, log_file_path, spark):
    try:
        suffix = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        file_path = f"{log_file_path.rstrip('/')}/log_{suffix}.txt"
        df = spark.createDataFrame([(message,)], ["message"])
        df.coalesce(1).write.mode("append").text(file_path)
    except Exception as e:
        print(f"Failed to log to S3: {e}")
        traceback.print_exc()


def log(message, log_file_path=None, spark=None, cloudwatch_group=None, cloudwatch_stream=None):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{ts}] {message}"
    log_to_console(full_message)

    if log_file_path and spark:
        log_to_s3(full_message, log_file_path, spark)

    if cloudwatch_group and cloudwatch_stream:
        log_to_cloudwatch(full_message, cloudwatch_group, cloudwatch_stream)


# ========================
# DYNAMODB WRITER
# ========================
def write_to_dynamodb(df, table_name):
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        records = df.toJSON().collect()
        for record in records:
            item = json.loads(record, parse_float=Decimal)
            table.put_item(Item=item)

        log(f"Wrote {len(records)} records to DynamoDB table: {table_name}")

    except Exception as e:
        log(f"Error writing to DynamoDB {table_name}: {e}")
        traceback.print_exc()
        raise
