import requests
from bs4 import BeautifulSoup
import decimal
import json
import datetime
import os
import sys
import re
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

def sanitize_price(price_str):
    """Extracts numeric value from price string."""
    digits = re.sub(r'\D', '', price_str)
    if digits:
        return int(digits)
    return 0

def append_to_csv(df, filepath='data/gold_prices.csv'):
    # Append directly to CSV without duplicate check
    file_exists = os.path.exists(filepath)
    df.to_csv(filepath, mode='a', header=not file_exists, index=False)
    print(f"✓ Successfully loaded {len(df)} rows to CSV")

def initialize_clients(SERVICE_ACCOUNT_KEY: str, PROJECT_ID: str):
    """Initialize BigQuery clients with service account credentials."""
    scopes = [
        'https://www.googleapis.com/auth/bigquery'
    ]
    creds = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_KEY,
        scopes=scopes
    )
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)
    return bq_client

def append_to_bigquery(df, bq_client, table_id):
    """Append DataFrame to BigQuery table."""
    try:
      job_config = bigquery.LoadJobConfig(
          write_disposition='WRITE_APPEND'
      )

      load_job = bq_client.load_table_from_dataframe(
              df, table_id, job_config=job_config
          )
      load_job.result()
      print(f"✓ Successfully loaded {len(df)} rows to BigQuery")
    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")
    return

def scrape_gold_prices(output_format='dataframe'):
    url = "https://www.24h.com.vn/gia-vang-hom-nay-c425.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching URL: {e}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table_container = soup.find("table", class_="gia-vang-search-data-table")
    
    if not table_container:
        print("Could not find gold price table container.")
        return None
    
    rows = table_container.find_all("tr")
    
    data_matrix = []
    
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 3:
            brand_node = cols[0].find("h2")
            brand = brand_node.get_text(strip=True) if brand_node else cols[0].get_text(strip=True)
            
            buy_node = cols[1].find("span", class_="fixW")
            sell_node = cols[2].find("span", class_="fixW")
            
            buy_raw = buy_node.get_text(strip=True) if buy_node else cols[1].get_text(strip=True)
            sell_raw = sell_node.get_text(strip=True) if sell_node else cols[2].get_text(strip=True)
            
            buy_price = sanitize_price(buy_raw)
            sell_price = sanitize_price(sell_raw)
            
            if brand and (buy_price > 0 or sell_price > 0):
                now_str = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).replace(microsecond=0, tzinfo = None)
                data_matrix.append([brand, buy_price, sell_price, now_str])
    
    df = pd.DataFrame(data_matrix, columns=["brand", "buy_price", "sell_price", "date"])

    df["brand"] = df["brand"].astype("string")
    #Helper function
    def to_bq_numeric(val):
        if pd.isna(val):
            return None
        return decimal.Decimal(str(val))
    
    df["buy_price"] = df["buy_price"].apply(to_bq_numeric)
    df["sell_price"] = df["sell_price"].apply(to_bq_numeric)
    
    df["date"] = pd.to_datetime(df["date"])
    
    if output_format == 'numpy':
        return np.array(data_matrix)
    elif output_format == 'array':
        return data_matrix
    else:
        return df

if __name__ == "__main__":
    df_prices = scrape_gold_prices(output_format='dataframe')
    if df_prices is not None and not df_prices.empty:
      # append to csv
      append_to_csv(df_prices)

      # append to bigquery
      sa_env = os.environ.get("GCP_SA_KEY")
      GCP_id_env = os.environ.get("GCP_PROJECT_ID")
      if not GCP_id_env:
          print("🚨 Lỗi: Không tìm thấy biến môi trường GCP_PROJECT_ID trên GitHub Secrets!")
          sys.exit(1)
      if not sa_env:
          print("🚨 Lỗi: Không tìm thấy biến môi trường GCP_SA_KEY trên GitHub Secrets!")
          sys.exit(1)
      sa_info = json.loads(sa_env)
      GCP_id = json.loads(GCP_id_env)

      bq_client = initialize_clients(SERVICE_ACCOUNT_KEY= sa_info, PROJECT_ID= GCP_id["Project ID"])
      append_to_bigquery(df = df_prices, bq_client= bq_client, table_id = GCP_id["Table ID"])
