import requests
import pandas as pd
import os
import json
from datetime import datetime
import logging
import boto3
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def s3_upload(data_frame : pd.DataFrame) -> None:
    try:
        logger.info("Uploading data to S3")
        bucket_name="nifty50-raw-layer"
        s3 = boto3.client("s3")
        csv_buffer = io.BytesIO()
        data_frame.to_csv(csv_buffer, index=False)  # Convert DataFrame to CSV format (in-memory)
        csv_buffer.seek(0)  # Reset buffer position
        s3.upload_fileobj(csv_buffer, bucket_name, f"macroeconimics/year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/macroeconomic_metrics.csv", ExtraArgs={'ContentType': 'text/csv'})
        logger.info("Uploaded macroeconomic_metrics.csv to S3")
    except Exception as e:
        logger.error(f"Error uploading macroeconomic_metrics.csv to S3 in s3_upload method with error --> {e}", exc_info=True)
        raise

def lambda_handler(event, context):
    logger.info("Lambda Execution Started")
    
    try:
        country_code = os.environ["COUNTRY_CODE"]
        base_url = os.environ["BASE_URL"]
        indicators = json.loads(os.environ["INDICATORS"])  # Convert JSON string to dictionary
        if not indicators:raise ValueError("INDICATORS is empty in environment variables")      
        all_data = []  # Use a list to store DataFrames before concatenation
        for indicator_name, indicator_id in indicators.items():
            api_url = f"{base_url}{indicator_id}/{country_code}"
            logger.info(f"Fetching data for {indicator_name} from {api_url}")
            try:
                response = requests.get(url=api_url, timeout=10)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logger.error(f"HTTP request failed for {indicator_name} ({indicator_id}) --> {repr(e)}", exc_info=True)
                raise
            except (ValueError, TypeError) as e:
                logger.error(f"Invalid JSON response for {indicator_name} ({indicator_id}) --> {repr(e)}", exc_info=True)
                raise
            try:
                values = data.get("values")
                if not isinstance(values, dict) or indicator_id not in values or country_code not in values[indicator_id]:
                    raise KeyError(f"Missing or malformed data for {indicator_name}")   
                
                if not isinstance(values,dict):raise ValueError(f"values is not a dictionary for {indicator_name}")
                if indicator_id not in values:raise KeyError(f"{indicator_id} not found in values for {indicator_name}")
                if country_code not in values[indicator_id]:raise KeyError(f"{country_code} not found in values[{indicator_id}] for {indicator_name}")
                values_dict = values[indicator_id][country_code]
                df = pd.DataFrame({
                    "indicator_name": indicator_name,
                    "year": values_dict.keys(),
                    "value": values_dict.values(),
                })
                all_data.append(df)  # Collect instead of concatenating in loop
            except Exception as e:
                logger.error(f"Error extracting data for {indicator_name} --> {repr(e)}", exc_info=True)
                raise
        if all_data:    
            combined_df = pd.concat(all_data, ignore_index=True)
            s3_upload(combined_df)
            logger.info("Lambda Execution Completed")
        else:
            logger.warning("No data fetched for any indicator")
            raise ValueError("No data fetched for any indicator")
    except Exception as e:
        logger.error(f"Lambda Execution Failed with error --> {repr(e)}", exc_info=True)
        raise
