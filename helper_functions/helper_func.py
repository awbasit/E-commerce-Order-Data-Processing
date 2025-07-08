import boto3
import os
import json
import traceback
import pandas as pd
from io import BytesIO
import datetime
from urllib.parse import urlparse
from decimal import Decimal
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError


# ========================
# SPARK INITIALIZATION
# ========================

def init_spark(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.autoBroadcastJoinThreshold", 104857600) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    print("=" * 50)
    print(f"Spark Version: {spark.version}")
    print(f"Spark App Name: {spark.sparkContext.appName}")
    print(f"Spark App ID: {spark.sparkContext.applicationId}")
    print("=" * 50)

    return spark

# ========================
# READ CSV FROM S3 (Pandas)
# ========================
def read_csv_from_s3(s3_path, sample_only=False):
    try:
        s3 = boto3.client("s3")
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()))

        if sample_only:
            log(f"Sampled 5 rows from: {s3_path}")
            return df.head(5)
        
        log(f"Loaded dataset from: {s3_path}")
        return df
    except Exception as e:
        log(f"Failed to read CSV from {s3_path}: {e}")
        traceback.print_exc()
        raise

# ========================
# VALIDATE AND ENRICH
# ========================
def check_referential_integrity(df_main, df_ref, main_key, ref_key, ref_name):
    valid_keys = set(df_ref[ref_key].dropna().unique())
    mask = df_main[main_key].isin(valid_keys)
    invalid_rows = df_main[~mask]

    if not invalid_rows.empty:
        log(f"Found {len(invalid_rows)} rows in '{main_key}' not found in '{ref_name}.{ref_key}'")
        return df_main[mask], invalid_rows
    return df_main, pd.DataFrame()


# ========================
# DYNAMODB TABLE CREATION
# ========================

def create_table_if_not_exists(table_name, key_schema, attribute_definitions, throughput):
    dynamodb = boto3.client("dynamodb")

    try:
        dynamodb.describe_table(TableName=table_name)
        print(f"Table '{table_name}' already exists.")
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"Creating DynamoDB table '{table_name}'...")

        try:
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput=throughput
            )

            waiter = dynamodb.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
            print(f"Table '{table_name}' created and ready.")

        except ClientError as e:
            print(f"Failed to create table '{table_name}': {e}")
            raise

# ========================
# VALIDATE AND ENRICH DATA
# ========================

def validate_and_enrich(df, dataset, bad_row_path, file_path, ref_data_paths={}):
    required_columns = {
        "order_items": ["order_id", "product_id", "user_id", "sale_price", "created_at", "status"],
        "products": ["id", "sku", "cost", "category", "name"],
        "orders": ["order_id", "user_id", "created_at", "status", "num_of_item"]
    }

    if dataset not in required_columns:
        raise ValueError(f"Unknown dataset: {dataset}")

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]
    required_cols = [c.lower() for c in required_columns[dataset]]

    # Check for missing required columns
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    # Drop nulls and duplicates
    original_len = len(df)
    df = df.dropna(subset=required_cols)
    after_null_len = len(df)
    df = df.drop_duplicates()
    after_dedup_len = len(df)

    log(f"{dataset}: Original={original_len}, After Nulls={after_null_len}, After Dedup={after_dedup_len}")

    # Referential Integrity Check (only for order_items)
    rejected_referential = pd.DataFrame()
    if dataset == "order_items":
        if "products" in ref_data_paths:
            df_products = (
                read_csv_from_s3(ref_data_paths["products"])
                if isinstance(ref_data_paths["products"], str)
                else ref_data_paths["products"]
            )
            df, bad = check_referential_integrity(df, df_products, "product_id", "id", "products")
            rejected_referential = pd.concat([rejected_referential, bad])

        if "orders" in ref_data_paths:
            df_orders = (
                read_csv_from_s3(ref_data_paths["orders"])
                if isinstance(ref_data_paths["orders"], str)
                else ref_data_paths["orders"]
            )
            df, bad = check_referential_integrity(df, df_orders, "order_id", "order_id", "orders")
            rejected_referential = pd.concat([rejected_referential, bad])

    # Save rejected rows
    if not rejected_referential.empty:
        ts = datetime.datetime.now() .strftime("%Y%m%d_%H%M%S")
        parsed = urlparse(bad_row_path)
        filename = os.path.basename(file_path).replace(".csv", "")
        rejected_key = f"{parsed.path.strip('/')}/{dataset}/{filename}_badrows_{ts}.csv"

        buffer = BytesIO()
        rejected_referential.to_csv(buffer, index=False)
        buffer.seek(0)

        s3 = boto3.client("s3")
        s3.put_object(Bucket=parsed.netloc, Key=rejected_key, Body=buffer)
        log(f"Rejected rows saved to: s3://{parsed.netloc}/{rejected_key}")

    # Save validated data to validated_data/<dataset> folder
    validated_ts = datetime.datetime.now() .strftime("%Y%m%d_%H%M%S")
    parsed_input = urlparse(file_path)
    file_name = os.path.basename(parsed_input.path).replace(".csv", "")
    output_key = f"validated_data/{dataset}/{file_name}_validated_{validated_ts}.csv"

    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=parsed_input.netloc, Key=output_key, Body=buffer)

    log(f"Validated data saved to: s3://{parsed_input.netloc}/{output_key}")

    return df

# ========================
# LOGGING UTILITIES
# ========================
def log_to_console(message):
    ts = datetime.datetime.now() .strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def log_to_cloudwatch(message, cloudwatch_group, cloudwatch_stream):
    try:
        logs_client = boto3.client("logs")
        ts_ms = int(datetime.datetime.utcnow().timestamp() * 1000)

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

def log(message, cloudwatch_group=None, cloudwatch_stream=None):
    ts = datetime.datetime.now() .strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{ts}] {message}"
    log_to_console(full_message)

    if cloudwatch_group and cloudwatch_stream:
        log_to_cloudwatch(full_message, cloudwatch_group, cloudwatch_stream)

# ========================
# DYNAMODB WRITER (for pandas)
# ========================
def write_to_dynamodb(df, table_name):
    def convert_types(record):
        for k, v in record.items():
            if isinstance(v, (datetime.date, datetime.datetime)):
                record[k] = v.isoformat()
            elif isinstance(v, float):
                record[k] = Decimal(str(v))
        return record

    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        records = [row.asDict() for row in df.collect()]
        for record in records:
            cleaned = convert_types(record)
            table.put_item(Item=cleaned)

        log(f"Wrote {len(records)} records to DynamoDB table: {table_name}")

    except Exception as e:
        log(f"Error writing to DynamoDB {table_name}: {e}")
        traceback.print_exc()
        raise