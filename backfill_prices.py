import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
from google.cloud import bigquery

# Use your active project
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "deft-gearbox-410821")
DATASET = "market"

TABLE_HOURLY = f"{PROJECT}.{DATASET}.prices_hourly"
TABLE_DAILY = f"{PROJECT}.{DATASET}.prices_daily"

# Change ticker if needed
TICKER = "HDFCBANK.NS"  # or "AAPL"


def download_hourly(days=30) -> pd.DataFrame:
    """Download last `days` of hourly OHLCV from Yahoo Finance."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    print(f"Downloading {TICKER} from {start} to {end} (1h interval)...")
    df = yf.download(
        TICKER,
        start=start,
        end=end,
        interval="1h",
        auto_adjust=False,
        progress=False,
        group_by="column",
    )

    if df.empty:
        print("No data returned from Yahoo Finance.")
        return pd.DataFrame()

    # Flatten MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # Index → column
    df = df.reset_index()

    # Normalize timestamp col name
    if "Datetime" in df.columns:
        df.rename(columns={"Datetime": "ts"}, inplace=True)
    elif "Date" in df.columns:
        df.rename(columns={"Date": "ts"}, inplace=True)
    else:
        df.rename(columns={df.columns[0]: "ts"}, inplace=True)

    df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["ticker"] = TICKER
    df["src"] = "yahoo"
    df["load_ts"] = datetime.now(timezone.utc)

    df = df[
        ["ts", "ticker", "open", "high", "low",
         "close", "adj_close", "volume", "src", "load_ts"]
    ]

    print("Hourly dataframe preview:")
    print(df.head())
    print(df.dtypes)

    return df


def write_hourly_to_bq(df: pd.DataFrame):
    """Insert hourly rows into prices_hourly (schema has src + load_ts)."""
    if df.empty:
        print("Hourly dataframe empty, skipping.")
        return

    client = bigquery.Client(project=PROJECT)
    rows = []

    for _, r in df.iterrows():
        rows.append(
            {
                "ts": r["ts"].to_pydatetime().isoformat() if pd.notna(r["ts"]) else None,
                "ticker": str(r["ticker"]),
                "open": float(r["open"]) if pd.notna(r["open"]) else None,
                "high": float(r["high"]) if pd.notna(r["high"]) else None,
                "low": float(r["low"]) if pd.notna(r["low"]) else None,
                "close": float(r["close"]) if pd.notna(r["close"]) else None,
                "adj_close": float(r["adj_close"]) if pd.notna(r["adj_close"]) else None,
                "volume": int(r["volume"]) if pd.notna(r["volume"]) else None,
                "src": str(r["src"]),
                "load_ts": r["load_ts"].to_pydatetime().isoformat()
                if pd.notna(r["load_ts"])
                else None,
            }
        )

    print(f"Inserting {len(rows)} rows into {TABLE_HOURLY}...")
    errors = client.insert_rows_json(TABLE_HOURLY, rows)

    if errors:
        print("BigQuery insert errors for hourly:", errors)
    else:
        print(f"Inserted {len(rows)} rows into {TABLE_HOURLY}")


def aggregate_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly → daily OHLCV."""
    if hourly_df.empty:
        print("Hourly dataframe empty, cannot aggregate daily.")
        return pd.DataFrame()

    df = hourly_df.copy()
    df["date"] = df["ts"].dt.floor("D")

    daily = df.groupby(["date", "ticker"]).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        adj_close=("adj_close", "last"),
        volume=("volume", "sum"),
    ).reset_index()

    daily.rename(columns={"date": "ts"}, inplace=True)
    daily["ts"] = pd.to_datetime(daily["ts"], utc=True)

    print("Daily aggregated dataframe preview:")
    print(daily.head())
    print(daily.dtypes)

    return daily


def write_daily_to_bq(df: pd.DataFrame):
    """
    Insert daily rows into prices_daily.
    ⚠ IMPORTANT: prices_daily schema does NOT have 'src' or 'load_ts',
    so we only send: ts, ticker, open, high, low, close, adj_close, volume.
    """
    if df.empty:
        print("Daily dataframe empty, skipping.")
        return

    client = bigquery.Client(project=PROJECT)
    rows = []

    for _, r in df.iterrows():
        rows.append(
            {
                "ts": r["ts"].to_pydatetime().isoformat() if pd.notna(r["ts"]) else None,
                "ticker": str(r["ticker"]),
                "open": float(r["open"]) if pd.notna(r["open"]) else None,
                "high": float(r["high"]) if pd.notna(r["high"]) else None,
                "low": float(r["low"]) if pd.notna(r["low"]) else None,
                "close": float(r["close"]) if pd.notna(r["close"]) else None,
                "adj_close": float(r["adj_close"]) if pd.notna(r["adj_close"]) else None,
                "volume": int(r["volume"]) if pd.notna(r["volume"]) else None,
            }
        )

    print(f"Inserting {len(rows)} rows into {TABLE_DAILY} (no src field)...")
    errors = client.insert_rows_json(TABLE_DAILY, rows)

    if errors:
        print("BigQuery insert errors for daily:", errors)
    else:
        print(f"Inserted {len(rows)} rows into {TABLE_DAILY}")


def main():
    print(f"Using project: {PROJECT}")
    print(f"Hourly table: {TABLE_HOURLY}")
    print(f"Daily table:  {TABLE_DAILY}")

    hourly = download_hourly(days=30)
    if hourly.empty:
        print("No data downloaded, exiting.")
        return

    write_hourly_to_bq(hourly)

    daily = aggregate_daily(hourly)
    write_daily_to_bq(daily)

    print("Backfill complete.")


if __name__ == "__main__":
    main()
