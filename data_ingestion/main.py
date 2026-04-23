# data_ingestion/main.py
# Reads ticker list from BigQuery → fetches yfinance .info → uploads JSON to GCS
# Runs on Compute Engine via BashOperator in ingestion_dag.py

from datetime import datetime
import yfinance as yf
from google.cloud import bigquery, storage
import yaml
import json

# Step 1: Config Loader
def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# Step 2: BigQuery Reader & Ticker Fetcher
def get_tickers(project_id, dataset, table):
    client = bigquery.Client(project=project_id)
    query  = f"SELECT DISTINCT ticker FROM `{project_id}.{dataset}.{table}`"
    df     = client.query(query).to_dataframe()
    return df["ticker"].dropna().tolist()

# Step 3: yfinance Fetcher through API
def fetch_ticker_info(ticker):
    """Fetch .info dict from yfinance. Returns None on error."""
    try:
        info = yf.Ticker(ticker).info
        return dict(info) if info else None
    except Exception as e:
        print(f"[WARN] Skipping {ticker}: {e}")
        return None

# Step 4: GCS Uploader
def upload_to_gcs(bucket_name, data, ticker, date):
    """
    Upload one ticker's JSON to GCS under a date-partitioned path.
    blob() takes a relative path inside the bucket — NOT a full gs:// URI.
    """
    client    = storage.Client()
    bucket    = client.bucket(bucket_name)
    blob_path = f"raw-yfinance/date={date}/{ticker}.json"   # relative path
    blob      = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")
    print(f"[INFO] Uploaded: gs://{bucket_name}/{blob_path}")

# Step 5: Orchestration & Control Table Updater
def run():
    config     = load_config()
    project_id = config["gcp"]["project_id"]
    bucket     = config["gcp"]["bucket"]
    dataset    = config["gcp"]["dataset"]
    table      = config["gcp"]["ticker_table"]
    today      = datetime.today().strftime(config["partition"]["date_format"])

    tickers = get_tickers(project_id, dataset, table)
    print(f"[INFO] Processing {len(tickers)} tickers for date={today}")

    for ticker in tickers:
        data = fetch_ticker_info(ticker)
        if data:
            upload_to_gcs(bucket, data, ticker, today)


if __name__ == "__main__":
    run()
