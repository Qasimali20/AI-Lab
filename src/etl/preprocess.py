"""
Preprocess CoinMarketCap raw data and append to DuckDB.
"""

import duckdb
import pandas as pd
import os

RAW_PATH = "data/coin_data_raw.csv"
DB_PATH = "data/coin_data.duckdb"

def preprocess_data():
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Raw file not found: {RAW_PATH}")

    print("🔹 Loading raw data...")
    df = pd.read_csv(RAW_PATH)

    # ✅ Safely extract columns
    def safe_col(col):
        return df[col] if col in df.columns else None

    df["price"] = safe_col("quote.USD.price")
    df["market_cap"] = safe_col("quote.USD.market_cap")
    df["volume_24h"] = safe_col("quote.USD.volume_24h")
    df["percent_change_1h"] = safe_col("quote.USD.percent_change_1h")
    df["percent_change_24h"] = safe_col("quote.USD.percent_change_24h")
    df["percent_change_7d"] = safe_col("quote.USD.percent_change_7d")

    keep_cols = [
        "fetch_time", "id", "name", "symbol", "cmc_rank",
        "price", "market_cap", "volume_24h",
        "percent_change_1h", "percent_change_24h", "percent_change_7d"
    ]
    df = df[[c for c in keep_cols if c in df.columns]].copy()

    df = df.dropna(subset=["price", "market_cap"])
    df = df[df["market_cap"] > 0]

    numeric_cols = [
        "price", "market_cap", "volume_24h",
        "percent_change_1h", "percent_change_24h", "percent_change_7d"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["fetch_time"] = pd.to_datetime(df["fetch_time"], errors="coerce")
    print(f"✅ Cleaned data: {len(df)} rows")

    # ✅ Append instead of overwrite
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)

    # Create table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS coins AS SELECT * FROM df LIMIT 0
    """)

    # Append new records
    con.register("temp_df", df)
    con.execute("INSERT INTO coins SELECT * FROM temp_df")
    con.close()

    print(f"💾 Appended {len(df)} new rows to {DB_PATH}")

if __name__ == "__main__":
    preprocess_data()
