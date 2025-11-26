import os
from datetime import datetime, timedelta, timezone

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)

import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tsa.statespace.sarimax import SARIMAX
from google.cloud import bigquery


# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "deft-gearbox-410821")
DATASET = "market"

TABLE_HOURLY = f"{PROJECT}.{DATASET}.prices_hourly"
TABLE_DAILY = f"{PROJECT}.{DATASET}.prices_daily"
TABLE_FORECAST_SHORT = f"{PROJECT}.{DATASET}.forecast_short"
TABLE_FORECAST_LONG = f"{PROJECT}.{DATASET}.forecast_long"
TABLE_EVAL = f"{PROJECT}.{DATASET}.eval_metrics"

TICKER = "HDFCBANK.NS"


# ------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------

def get_client():
    return bigquery.Client(project=PROJECT)


def compute_metrics(y_true, y_pred):
    """Compute MAE, RMSE, MAPE and 5% mean benchmark."""
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)

    mae = mean_absolute_error(y_true, y_pred)

    mse = mean_squared_error(y_true, y_pred)
    rmse = float(np.sqrt(mse))

    mape = float(np.mean(np.abs((y_true - y_pred) / np.maximum(np.abs(y_true), 1e-8))) * 100)

    mean_price = float(np.mean(y_true))
    benchmark_5pct = mean_price * 0.05
    passed = rmse <= benchmark_5pct

    return {
        "mae": float(mae),
        "rmse": float(rmse),
        "mape": float(mape),
        "mean_price": mean_price,
        "benchmark_5pct_mean": float(benchmark_5pct),
        "rmse_vs_5pct_mean_pass": bool(passed),
    }


def insert_rows(client, table, rows):
    if not rows:
        print(f"No rows to insert into {table}")
        return
    errors = client.insert_rows_json(table, rows)
    if errors:
        print(f"BigQuery insert errors for {table}: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into {table}")


def write_metric(client, model, horizon, metrics):
    row = {
        "model": model,
        "horizon": horizon,
        "rmse": metrics["rmse"],
        "mae": metrics["mae"],
        "mape": metrics["mape"],
        "mean_price": metrics["mean_price"],
        "benchmark_5pct_mean": metrics["benchmark_5pct_mean"],
        "rmse_vs_5pct_mean_pass": metrics["rmse_vs_5pct_mean_pass"],
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    insert_rows(client, TABLE_EVAL, [row])


# ------------------------------------------------------------------
# SHORT-TERM (24h) SARIMA FORECAST
# ------------------------------------------------------------------

def run_short_term(client: bigquery.Client):
    print("Short-term forecast...")

    sql = f"""
    SELECT ts, adj_close
    FROM `{TABLE_HOURLY}`
    WHERE ticker = '{TICKER}'
    ORDER BY ts
    """
    df = client.query(sql).result().to_dataframe()

    if df.empty:
        print("No hourly data.")
        return

    # Ensure datetime and sort
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    # ---- FIX DUPLICATES ----
    df["ts"] = df["ts"].dt.floor("h")      # round to hour
    df = df.groupby("ts").agg({"adj_close": "last"}).reset_index()
    df = df.sort_values("ts")

    # Set index
    df = df.set_index("ts")

    # ---- SAFE ASFREQ ----
    df = df.asfreq("h")  # now valid → no duplicate index labels

    if df["adj_close"].isna().sum() > 0:
        # fill missing hours due to market holidays or NA gaps
        df["adj_close"] = df["adj_close"].ffill()

    y = df["adj_close"].astype(float)

    if len(y) <= 24:
        print(f"Not enough hourly points ({len(y)}) for 24h forecast.")
        return

    # last 24 hrs = test
    train = y.iloc[:-24]
    test = y.iloc[-24:]

    model = SARIMAX(train, order=(1, 1, 1), seasonal_order=(1, 0, 1, 24))
    res = model.fit(disp=False, maxiter=500)


    forecast = res.get_forecast(steps=24).predicted_mean

    # Metrics
    metrics = compute_metrics(test.values, forecast.values)
    print("Short-term metrics:", metrics)
    write_metric(client, "sarima_short_term", "24h", metrics)

    # Future timestamps
    last_ts = df.index.max()
    future_index = pd.date_range(
        last_ts + timedelta(hours=1),
        periods=24,
        freq="h",
        tz="UTC",
    )

    now_iso = datetime.now(timezone.utc).isoformat()

    rows = [
        {
            "ts": ts_val.to_pydatetime().isoformat(),
            "ticker": TICKER,
            "forecast": float(yhat),
            "horizon": "24h",
            "created_ts": now_iso,
        }
        for ts_val, yhat in zip(future_index, forecast.values)
    ]

    insert_rows(client, TABLE_FORECAST_SHORT, rows)


# ------------------------------------------------------------------
# LONG-TERM (90d) PROPHET FORECAST
# ------------------------------------------------------------------

def run_long_term(client: bigquery.Client):
    print("Long-term forecast...")

    sql = f"""
    SELECT ts AS ds, adj_close AS y
    FROM `{TABLE_DAILY}`
    WHERE ticker = '{TICKER}'
    ORDER BY ds
    """
    df = client.query(sql).result().to_dataframe()

    if df.empty:
        print("No daily data.")
        return

    df["ds"] = pd.to_datetime(df["ds"], utc=True).dt.tz_localize(None)
    df["y"] = df["y"].astype(float)

    n = len(df)
    print(f"Daily rows for {TICKER}: {n}")

    if n < 10:
        print("Not enough daily data for long-term forecast.")
        return

    # Split train/test
    if n > 120:
        df_train = df.iloc[:-90]
        df_test = df.iloc[-90:]
        eval_horizon = "90d"
    else:
        df_train = df.iloc[:-7]
        df_test = df.iloc[-7:]
        eval_horizon = "7d (limited data)"

    m = Prophet(
        changepoint_prior_scale=0.015,
        seasonality_mode="multiplicative",
    )
    m.fit(df_train)

    # Predict next 90 days
    future = m.make_future_dataframe(periods=90)
    forecast = m.predict(future)

    # Evaluate on overlap
    merged = pd.merge(
        df_test[["ds", "y"]],
        forecast[["ds", "yhat"]],
        on="ds",
        how="inner",
    ).sort_values("ds")

    if not merged.empty:
        metrics = compute_metrics(merged["y"], merged["yhat"])
        print("Long-term metrics:", metrics)
        write_metric(client, "prophet_long_term", eval_horizon, metrics)
    else:
        print("No overlapping rows between forecast and test; skipping metrics.")

    # Only future 90 days → forecast_long
    max_hist = df["ds"].max()
    future_only = forecast[forecast["ds"] > max_hist].copy()

    now_iso = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "ds": r["ds"].to_pydatetime().isoformat(),
            "ticker": TICKER,
            "yhat": float(r["yhat"]),
            "yhat_lower": float(r["yhat_lower"]),
            "yhat_upper": float(r["yhat_upper"]),
            "created_ts": now_iso,
        }
        for _, r in future_only.iterrows()
    ]

    insert_rows(client, TABLE_FORECAST_LONG, rows)


# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------

def main():
    client = get_client()
    run_short_term(client)
    run_long_term(client)
    print("Forecasting complete.")


if __name__ == "__main__":
    main()
