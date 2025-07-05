import sys
import traceback
from datetime import datetime
from pyspark.sql.functions import col, to_date, countDistinct, count, sum as _sum, avg, when
from helper.helper import init_spark, log, write_to_dynamodb
import boto3

# === CONFIGURATION ===
VALIDATED_S3_PATH = "s3://project6dt/validated/order_items"
LOG_PATH = "s3://project6dt/logs/transformation"
CLOUDWATCH_GROUP = "ecommerce-pipeline-log-group"
CLOUDWATCH_STREAM = "transformation-stream"
CATEGORY_KPI_TABLE = "CategoryKPI"
ORDER_KPI_TABLE = "OrderKPI"
REGION = "us-east-1"


def compute_category_kpis(df):
    return df.withColumn("order_date", to_date(col("created_at"))) \
        .groupBy("category", "order_date") \
        .agg(
            _sum("sale_price").alias("daily_revenue"),
            avg("sale_price").alias("avg_order_value"),
            (count(when(col("status") == "returned", True)) / count("*") * 100).alias("avg_return_rate")
        )


def compute_order_kpis(df):
    return df.withColumn("order_date", to_date(col("created_at"))) \
        .groupBy("order_date") \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            _sum("sale_price").alias("total_revenue"),
            count("*").alias("total_items_sold"),
            (count(when(col("status") == "returned", True)) / count("*") * 100).alias("return_rate"),
            countDistinct("user_id").alias("unique_customers")
        )


def run_transformation():
    spark = None
    try:
        spark = init_spark("Glue-Transformer")

        log("Transformer Job Started", spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # Load only order_items dataset (others might be used in extended cases)
        df = spark.read.option("header", True).csv(VALIDATED_S3_PATH)
        row_count = df.count()
        log(f"Loaded {row_count} rows from {VALIDATED_S3_PATH}", spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # Drop duplicates
        df = df.dropDuplicates()
        log(f"Row count after deduplication: {df.count()}", spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # Compute KPIs
        cat_kpis_df = compute_category_kpis(df)
        order_kpis_df = compute_order_kpis(df)

        log("Computed Category-Level KPIs", spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)
        log("Computed Order-Level KPIs", spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # Write KPIs to DynamoDB
        write_to_dynamodb(cat_kpis_df, CATEGORY_KPI_TABLE)
        write_to_dynamodb(order_kpis_df, ORDER_KPI_TABLE)

        log(f"Successfully stored KPIs into DynamoDB tables: {CATEGORY_KPI_TABLE}, {ORDER_KPI_TABLE}", 
            spark=spark, cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

    except Exception as e:
        log(f"ERROR during transformation: {e}", spark=spark, log_file_path=LOG_PATH,
            cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)
        traceback.print_exc()
        raise

    finally:
        if spark:
            spark.stop()
        log("Transformer Job Completed", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)


if __name__ == "__main__":
    run_transformation()
