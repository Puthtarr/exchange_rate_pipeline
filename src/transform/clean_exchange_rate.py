import os
import sys
import json
import boto3
import logging
import itertools
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, lit, create_map, expr, trim, to_date, to_timestamp

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.settings import raw_data, date, log_dir


load_dotenv()

# Setup logging (local)
logging.basicConfig(
    filename=os.path.join(log_dir, f'clean_data_from_minio.log'),
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("ReadExchangeRate")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", os.getenv("MINIO_USE_SSL", "false"))
        .getOrCreate()
    )
    return spark

# -------------------------------
# Clean and Transform Function
# -------------------------------
def clean_exchange_rate_df(df):
    cleaned = (
        df.filter((col("rate").isNotNull()) & (trim(col("rate")) != "") & (trim(col("rate")) != "na"))
          .dropDuplicates()
          .withColumn("rate", col("rate").cast(DoubleType()))
          .filter(col("rate") > 0)
          .withColumn("date", to_timestamp(col("date")))
    )
    return cleaned

# -------------------------------
# Load from MinIO, Clean, Save
# -------------------------------
def load_json_from_minio(spark, bucket_name: str, prefix: str):
    object_path = f"s3a://{bucket_name}/{prefix}/exchange_rate_{date}.json"

    try:
        df = spark.read.option("multiline", "true").json(object_path)
        logging.info(f"Successfully read JSON from {object_path}")
    except Exception as e:
        logging.error(f"Failed to read JSON: {e}")
        raise

    # ตรวจสอบว่า rates เป็น struct ที่มี field อยู่
    try:
        rate_fields = df.select("rates").schema[0].dataType.names
        if not rate_fields:
            logging.warning("No fields found in 'rates' struct")
            return
    except Exception as e:
        logging.error(f"Error extracting fields from 'rates': {e}")
        return

    # แปลง struct => map
    kv_pairs = list(itertools.chain.from_iterable(
        [(lit(f), col("rates").getField(f)) for f in rate_fields]
    ))
    df_with_map = df.select("date", "base", create_map(*kv_pairs).alias("rates_map"))

    # explode และ clean
    exploded_df = df_with_map.select(
        "date", "base", expr("explode(rates_map) as (currency, rate)")
    )

    cleaned_df = clean_exchange_rate_df(exploded_df)

    # Old Version
    # output_path = f"s3a://{bucket_name}/validated/exchange_rate_{date}.csv"
    # try:
    #     cleaned_df.write.mode("overwrite").option("header", True).csv(output_path)
    #     logging.info(f"Successfully wrote cleaned data to {output_path}")
    #
    #     return cleaned_df
    # except Exception as e:
    #     logging.error(f"Failed to write CSV: {e}")
    #     raise

    output_path = f"s3a://{bucket_name}/validated/exchange_rate_{date}.csv"
    try:
        cleaned_df.write.mode("overwrite").option("header", True).csv(output_path)
        logging.info(f"Successfully wrote cleaned data to {output_path}")

        return cleaned_df
    except Exception as e:
        logging.error(f"Failed to write CSV: {e}")
        raise

# -------------------------------
# Entry Point
# -------------------------------
if __name__ == "__main__":
    spark = create_spark_session()
    load_json_from_minio(spark, bucket_name="exchange.rate", prefix="raw_json")
