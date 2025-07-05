import boto3
import traceback
import pandas as pd
from io import BytesIO
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException


# ========================
# SPARK SESSION INITIALIZER
# ========================
def init_spark(app_name="BigDataApp"):
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        log(f"Initialized Spark Session: {spark.version}", spark=spark)
        return spark
    except Exception as e:
        log(f"ERROR initializing Spark: {e}")
        raise


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
def validate_and_enrich(df, dataset, bad_row_path=None, spark=None):
    try:
        REQUIRED_COLUMNS = {
            "order_items": ["order_id", "product_id", "user_id", "sale_price", "created_at", "status"],
            "products": ["id", "sku", "cost", "category", "name", "brand", "retail_price", "department"]
        }

        if dataset not in REQUIRED_COLUMNS:
            raise ValueError(f"Unknown dataset type: {dataset}")

        required_cols = set(map(str.lower, REQUIRED_COLUMNS[dataset]))
        actual_cols = set(map(str.lower, df.columns))
        missing = list(required_cols - actual_cols)

        if missing:
            raise ValueError(f"Missing required columns for {dataset}: {missing}")

        # Drop rows with nulls in required columns
        filtered_df = df
        for col_name in REQUIRED_COLUMNS[dataset]:
            filtered_df = filtered_df.filter(col(col_name).isNotNull())

        filtered_df = filtered_df.dropDuplicates()

        # Get bad rows
        bad_rows = df.subtract(filtered_df)

        # Write bad rows to audit folder if any
        if bad_rows and bad_row_path:
            ts_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            full_path = f"{bad_row_path.rstrip('/')}/bad_rows_{dataset}_{ts_suffix}.csv"
            bad_rows.write.mode("overwrite").option("header", True).csv(full_path)
            log(f"Wrote bad rows to: {full_path}", spark=spark)

        log(f"Validation passed for '{dataset}' - Valid records: {filtered_df.count()}", spark=spark)
        return filtered_df

    except Exception as e:
        log(f"Validation failed for {dataset}: {e}", spark=spark)
        traceback.print_exc()
        raise


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

        # Create log group
        try:
            logs_client.create_log_group(logGroupName=cloudwatch_group)
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        # Create log stream
        try:
            logs_client.create_log_stream(
                logGroupName=cloudwatch_group,
                logStreamName=cloudwatch_stream
            )
        except logs_client.exceptions.ResourceAlreadyExistsException:
            pass

        # Get sequence token
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
