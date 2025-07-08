import sys
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, count, sum as _sum, avg, when
from helper_functions.helper_func import init_spark, log, write_to_dynamodb, create_table_if_not_exists

# === CONFIGURATION ===
VALIDATED_S3_BASE_PATH = "s3a://project6dt/validated_data"
LOG_PATH = "s3a://project6dt/logs/transformation"
CLOUDWATCH_GROUP = "ecommerce-pipeline-log-group"
CLOUDWATCH_STREAM = "transformation-stream"
CATEGORY_KPI_TABLE = "CategoryKPI"
ORDER_KPI_TABLE = "OrderKPI"

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

        # === Ensure DynamoDB tables exist ===
        log("Ensuring DynamoDB tables exist", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)
        create_table_if_not_exists(
            CATEGORY_KPI_TABLE,
            key_schema=[
                {'AttributeName': 'category', 'KeyType': 'HASH'},
                {'AttributeName': 'order_date', 'KeyType': 'RANGE'}
            ],
            attribute_definitions=[
                {'AttributeName': 'category', 'AttributeType': 'S'},
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            throughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        create_table_if_not_exists(
            ORDER_KPI_TABLE,
            key_schema=[
                {'AttributeName': 'order_date', 'KeyType': 'HASH'}
            ],
            attribute_definitions=[
                {'AttributeName': 'order_date', 'AttributeType': 'S'}
            ],
            throughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )


        log("Transformer Job Started", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # === Load datasets ===
        order_items_path = f"{VALIDATED_S3_BASE_PATH}/order_items/"
        products_path = f"{VALIDATED_S3_BASE_PATH}/products/"
        orders_path = f"{VALIDATED_S3_BASE_PATH}/orders/"

        df_order_items = spark.read.parquet(order_items_path)
        df_products = spark.read.parquet(products_path)
        df_orders = spark.read.parquet(orders_path)

        log("Loaded validated datasets", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # === Enrich order_items ===
        df_enriched = df_order_items \
            .join(df_products.select("id", "category"), df_order_items["product_id"] == df_products["id"], "inner") \
            .join(df_orders.select("order_id", "created_at", "user_id"), "order_id", "inner") \
            .drop(df_products["id"])  # Remove duplicate product id

        log(f"Enriched data row count: {df_enriched.count()}", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # === Compute KPIs ===
        cat_kpis_df = compute_category_kpis(df_enriched)
        order_kpis_df = compute_order_kpis(df_enriched)

        log("Computed Category-Level KPIs", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)
        log("Computed Order-Level KPIs", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

        # === Write to DynamoDB ===
        write_to_dynamodb(cat_kpis_df, CATEGORY_KPI_TABLE)
        write_to_dynamodb(order_kpis_df, ORDER_KPI_TABLE)

        log(f"KPIs stored to DynamoDB tables: {CATEGORY_KPI_TABLE}, {ORDER_KPI_TABLE}",
            cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)

    except Exception as e:
        log(f"ERROR during transformation: {e}",
            cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)
        traceback.print_exc()
        raise

    finally:
        if spark:
            spark.stop()
        log("Transformer Job Completed", cloudwatch_group=CLOUDWATCH_GROUP, cloudwatch_stream=CLOUDWATCH_STREAM)


if __name__ == "__main__":
    run_transformation()