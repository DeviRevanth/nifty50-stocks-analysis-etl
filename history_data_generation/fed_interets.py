import requests
import pandas as pd
import os 
import logging
import io
import boto3
from datetime import datetime

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def s3_upload(data_frame: pd.DataFrame) -> None:
    """Uploads a DataFrame as a CSV to an S3 bucket with date-based partitioning."""
    try:
        if data_frame.empty:
            logger.warning("No data to upload. DataFrame is empty.")
            return  # Prevent uploading empty files

        logger.info("Uploading data to S3")
        bucket_name = "nifty50-raw-layer"
        s3 = boto3.client("s3")
        
        # Convert DataFrame to CSV format (in-memory)
        csv_buffer = io.BytesIO()
        data_frame.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Reset buffer position
        
        # Create S3 path with dynamic date partitions
        s3_key = (
            f"us_fed_rates/year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/us_fed_rates.csv")

        s3.upload_fileobj(csv_buffer, bucket_name, s3_key, ExtraArgs={'ContentType': 'text/csv'})
        logger.info(f"Uploaded us_fed_rates.csv to S3 at {s3_key}")

    except Exception as e:
        logger.error(f"Error uploading to S3: {e}", exc_info=True)
        raise

def lambda_handler(event, context):
    """AWS Lambda entry point for fetching US Fed Rates and storing in S3."""
    try:
        api_key = os.environ["API_KEY"]
        url_template = os.environ["URL"]
        api_url = url_template.replace("{API_KEY}", api_key)

        logger.info(f"Fetching data from {api_url}")

        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request failed for {api_url} with error --> {repr(e)}", exc_info=True)
        raise
    except (ValueError, TypeError) as e:
        logger.error(f"Invalid JSON response from {api_url} with error --> {repr(e)}", exc_info=True)
        raise

    try:
        observations = data.get("observations", [])
        if not observations:
            logger.warning("No observations found in API response.")
            return
        df = pd.DataFrame(observations)
        logger.info(f"Fetched {len(df)} records.")
        s3_upload(df)
        logger.info("Lambda execution completed successfully.")
    except Exception as e:
        logger.error(f"Error processing data in lambda_handler: {e}", exc_info=True)
        raise