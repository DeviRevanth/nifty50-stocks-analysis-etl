from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
import io
import boto3

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

date = datetime.now() - timedelta(days=365*10)
start_date = date.strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")

def fetch_stock_price(stock_name: str) -> pd.DataFrame:
    try:
        logger.info(f"Fetching data for {stock_name}...")
        stock = yf.Ticker(f"{stock_name.upper()}.NS")

        # Fetch historical data
        data = stock.history(start=start_date, end=end_date)

        # Fetch fundamental stock metrics
        stock_info = stock.info if stock.info else {}

        # If stock_info is None or empty, log a warning and return an empty DataFrame
        if not stock_info:
            logger.warning(f"No stock info found for {stock_name}")
            return pd.DataFrame()
        metadata = {
            "stock_name": stock_name,
            "market_cap": stock_info.get("marketCap"),
            "beta": stock_info.get("beta"),
            "pe_ratio": stock_info.get("trailingPE"),
            "dividend_yield": stock_info.get("dividendYield"),
            "roe": stock_info.get("returnOnEquity"),
            "roce": stock_info.get("returnOnCapitalEmployed"),
            "debt_to_equity": stock_info.get("debtToEquity"),
            "interest_coverage": stock_info.get("interestCoverage"),
            "pledged_percentage": stock_info.get("pledgeRatio"),
            "book_value": stock_info.get("bookValue"),
            "price_to_book": stock_info.get("priceToBook"),
            "price_to_earnings": stock_info.get("trailingPE"),
            "forward_pe": stock_info.get("forwardPE"),
            "enterprise_value": stock_info.get("enterpriseValue"),
            "ev_to_revenue": stock_info.get("enterpriseToRevenue"),
            "ev_to_ebitda": stock_info.get("enterpriseToEbitda"),
        }

        # Append metadata to DataFrame if data is not empty
        if not data.empty:
            for key, value in metadata.items():
                data[key] = value
        return data
    except Exception as e:
        logger.error(f"Error fetching {stock_name}: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame instead of raising an error
    
def s3_upload(data_frame : pd.DataFrame) -> None:
    try:
        logger.info("Uploading data to S3")
        bucket_name="nifty50-raw-layer"
        s3 = boto3.client("s3")
        csv_buffer = io.BytesIO()
        data_frame.to_csv(csv_buffer, index=False)  # Convert DataFrame to CSV format (in-memory)
        csv_buffer.seek(0)  # Reset buffer position
        s3.upload_fileobj(csv_buffer, bucket_name, f"stocks_data/history/year={datetime.today().strftime('%Y')}/month={datetime.today().strftime('%m')}/day={datetime.today().strftime('%d')}/nifty50_stock_history_data.csv", ExtraArgs={'ContentType': 'text/csv'})
        logger.info("Uploaded nifty50_stock_history_data.csv to S3")
    except Exception as e:
        logger.error(f"Error uploading to S3 in s3_upload method with error --> {e}", exc_info=True)
        raise
    
def lambda_handler(event, context):
    try:
        logger.info("Lambda execution  started")
        with open("nifty50_stock_names.json", "r") as data:
            nifty_50_stocks = json.load(data)
        nifty_50_symbols = nifty_50_stocks["stock_names"]
        main_df_list = []

        # Reduce parallel requests to avoid timeouts
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_stock_price, stock): stock for stock in nifty_50_symbols}
            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    main_df_list.append(result)
        if main_df_list:
            main_df = pd.concat(main_df_list)
            main_df.reset_index(inplace=True)
            main_df.columns = pd.Series(main_df.columns).str.lower().str.replace(" ", "_", regex=True)

            # Convert to JSON for Lambda return
            s3_upload(main_df)
            # return main_df.to_json(orient="records")
        else:
            return {"message": "No data retrieved"}
    except Exception as e:
        logger.error(f"Error in lambda_handler method with error --> {e}", exc_info=True)
        raise