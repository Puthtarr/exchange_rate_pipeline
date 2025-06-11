import os
import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

load_dotenv()

# def download_from_minio(bucket_name: str, object_name: str, save_path: str)