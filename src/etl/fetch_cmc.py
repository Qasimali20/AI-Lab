"""
Fetch latest cryptocurrency listings from CoinMarketCap
and save as CSV for further preprocessing and analytics.
"""
import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

# Load API key
load_dotenv()
API_KEY = os.getenv("CMC_API_KEY")

if not API_KEY:
    raise SystemExit("❌ API key not found. Please set CMC_API_KEY in .env file")

# API configuration
BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
HEADERS = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": API_KEY}
TIMEOUT = 20


def create_session():
    """Create a session with retry mechanism"""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session


def fetch_listings(limit=200, convert="USD"):
    """Fetch cryptocurrency listings"""
    params = {"start": 1, "limit": limit, "convert": convert}
    session = create_session()
    response = session.get(BASE_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    data = response.json()
    if "data" not in data:
        raise ValueError("Unexpected response structure: 'data' key missing.")
    return pd.json_normalize(data["data"])


def save_to_csv(df, path="data/coin_data_raw.csv"):
    """Save dataframe to CSV"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"✅ Saved {len(df)} rows → {path}")


if __name__ == "__main__":
    print("🔹 Fetching latest cryptocurrency listings...")
    df = fetch_listings(limit=200)
    df["fetch_time"] = pd.Timestamp.utcnow()
    save_to_csv(df)
    print("🔹 Preview:")
    print(df[["name", "symbol", "cmc_rank", "quote.USD.price"]].head())
