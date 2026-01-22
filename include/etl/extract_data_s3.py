import pandas as pd
import s3fs

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ..logger import setup_logger

logging = setup_logger("etl.extract_data_s3")

def get_storage_options(aws_conn_id: str):
    """
    Return AWS credentials
    """
    try:
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        creds = s3_hook.get_credentials()
    except Exception as e:
        logging.exception("Failed to get AWS credentials")
        raise RuntimeError(f"Could not retrieve AWS credentials for {aws_conn_id}") from e

    storage_options = {
        "key": creds.access_key,
        "secret": creds.secret_key,
    }

    return s3_hook, storage_options

def extract_data_from_s3(bucket: str, folder: str, aws_conn_id: str) -> dict:
    """
    Extract data from an S3 bucket
    """
    s3_hook, storage_options = get_storage_options(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket, prefix=folder)

    if not keys:
        raise ValueError(f"No files found in bucket! {bucket} with prefix {folder}")

    dfs= {}

    for key in keys:
        if not key.lower().endswith(".csv"):
            logging.info(f"Skipping file {key} / not csv")
            continue

        s3_path = f"s3://{bucket}/{key}"
        logging.info(f"Extracting data from {s3_path}")

        try:
            df = pd.read_csv(s3_path, storage_options=storage_options)

            if df.empty:
                logging.info(f"Skipping file {key} / empty")
                continue
        except Exception as e:
            logging.error(f"Skipping {s3_path}: {e}")
            raise

        dfs[key] = df

    return dfs
