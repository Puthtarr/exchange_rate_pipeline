import os
import sys
import logging
import duckdb
import pandas as pd
import socket
from dotenv import load_dotenv
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.settings import raw_data, date, log_dir

def can_connect(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except:
        return False

from dotenv import load_dotenv  # <- ไม่จำเป็นอีกต่อไป, ลบได้

def load_env():
    # ไม่ต้อง load_dotenv()
    mode = "LOCAL" if can_connect("localhost", 9000) else "AIRFLOW"
    print(f"⚙️ Running in {mode} mode")

    config = {
        "MINIO_ENDPOINT": os.getenv(f"MINIO_ENDPOINT_{mode}"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
        "POSTGRES_HOST": os.getenv(f"POSTGRES_HOST_{mode}"),
        "POSTGRES_PORT": os.getenv(f"POSTGRES_PORT_{mode}"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB"),
    }

    missing = [k for k, v in config.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")
    return config

def setup_logging():
    logging.basicConfig(
        filename=os.path.join(log_dir, 'upload_to_postgres.log'),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

def upload_to_postgres(csv_path: str, table_name: str):
    config = load_env()
    setup_logging()

    os.environ["AWS_ACCESS_KEY_ID"] = config["MINIO_ROOT_USER"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["MINIO_ROOT_PASSWORD"]

    db = duckdb.connect()
    db.execute("INSTALL httpfs;")
    db.execute("LOAD httpfs;")
    db.execute("INSTALL postgres;")
    db.execute("LOAD postgres;")

    db.execute("SET s3_region='us-east-1';")
    db.execute(f"SET s3_endpoint='{config['MINIO_ENDPOINT']}';")
    db.execute("SET s3_url_style='path';")
    db.execute("SET s3_use_ssl=false;")

    logging.info(f"Reading from: {csv_path}")
    df = db.execute(f"SELECT * FROM '{csv_path}'").fetchdf()
    df["date"] = pd.to_datetime(df["date"]).dt.date  # ให้แน่ใจว่าเป็น date

    print(df.columns)
    print(df.head())

    pg_conn_str = (
        f"postgresql://{config['POSTGRES_USER']}:{config['POSTGRES_PASSWORD']}"
        f"@{config['POSTGRES_HOST']}:{config['POSTGRES_PORT']}/{config['POSTGRES_DB']}"
    )
    db.execute(f"ATTACH '{pg_conn_str}' AS postgres (TYPE postgres);")

    db.execute(f"""
        CREATE TABLE IF NOT EXISTS postgres.public.{table_name} (
            date DATE,
            base TEXT,
            currency TEXT,
            rate FLOAT
        );
    """)

    db.register("df_to_insert", df)

    # ลบเฉพาะ row ที่มีวันที่ซ้ำกับใน df
    dates = tuple(df["date"].unique())
    date_list = ",".join([f"'{d}'" for d in dates])
    db.execute(f"""
        DELETE FROM postgres.public.{table_name}
        WHERE date IN ({date_list});
    """)

    db.execute(f"INSERT INTO postgres.public.{table_name} SELECT * FROM df_to_insert;")

    logging.info("Upload to PostgreSQL completed.")

if __name__ == "__main__":
    upload_to_postgres(
        "s3://exchange.rate/validated/exchange_rate_2025-06-17.csv/exchange_rate_2025-06-17.csv",
        "exchange_rate"
    )