import os
import logging
# from src.settings import log_dir, raw_data, date
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError,ClientError
import glob
from datetime import datetime
import socket

date = datetime.now().strftime('%Y-%m-%d')
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
data_dir = os.path.join(project_dir, "data")
log_dir = os.path.join(project_dir, 'logging')
raw_data = os.path.join(data_dir, 'raw')
os.makedirs(data_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)
os.makedirs(raw_data, exist_ok=True)

# load ENV
load_dotenv()

# Setup logging
logging.basicConfig(
    filename=os.path.join(log_dir, f'datalake_minio_uploader.log'),
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)

def can_connect(host, port):
    try:
        with socket.create_connection((host, int(port)), timeout=2):
            return True
    except:
        return False

def get_minio_client():
    required_env = ["MINIO_ENDPOINT_AIRFLOW", "MINIO_ENDPOINT_LOCAL", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_USE_SSL"]
    missing = [var for var in required_env if os.getenv(var) is None]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

    mode = "LOCAL" if can_connect("localhost", 9000) else "AIRFLOW"
    print(f"Mode : {mode}")

    endpoint = os.getenv(f"MINIO_ENDPOINT_{mode}")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    use_ssl = os.getenv("MINIO_USE_SSL").lower() == "true"

    return boto3.client(
        "s3",
        endpoint_url=f"http{'s' if use_ssl else ''}://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=use_ssl
    )

def upload_to_minio(file_path: str, bucket_name: str, object_name: str = None, prefix: str = "raw_json"):
    if object_name is None:
        filename = os.path.basename(file_path)
        object_name = f"{prefix}/{filename}" if prefix else filename

    try:
        s3_client = get_minio_client()

        # Check and create bucket if needed
        buckets = s3_client.list_buckets()
        if not any(b["Name"] == bucket_name for b in buckets.get("Buckets", [])):
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Created new bucket: {bucket_name}")

        # Upload file
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"Uploaded {file_path} to bucket '{bucket_name}' as '{object_name}'")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except NoCredentialsError:
        logging.error("MinIO credentials not found in environment")
        raise
    except ClientError as e:
        logging.error(f"MinIO ClientError: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to upload to MinIO: {e}")
        raise


def remove_local_file():
    logging.info('Start cleaning up raw local files')
    list_file = glob.glob(os.path.join(raw_data, '*'))

    for file in list_file:
        try:
            if os.path.isfile(file):
                os.remove(file)
                print(f'Deleted: {file}')
                logging.info(f'Deleted: {file}')
        except Exception as ex:
            print(f'Error: {ex}')
            logging.exception(f'Error while deleting {file}')


if __name__ == "__main__":
    sample_file = os.path.join(raw_data, f"exchange_rate_{date}.json")
    upload_to_minio(sample_file, "exchange.rate")
    remove_local_file()