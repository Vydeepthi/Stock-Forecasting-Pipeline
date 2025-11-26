import base64
import json
from datetime import datetime, timezone

import pandas as pd
from flask import Flask, request, jsonify
from google.cloud import bigquery
from statsmodels.tsa.statespace.sarimax import SARIMAX

app = Flask(__name__)
bq = bigquery.Client()

# Hard-coded for now – matches your project + create_tables.sql
PROJECT = "group5-stock-forecasting"
DS = "market"

T_H = f"{PROJECT}.{DS}.prices_hourly"
OUT_S = f"{PROJECT}.{DS}.forecast_short"
OUT_L = f"{PROJECT}.{DS}.forecast_long"


def read_hourly(ticker: str, days: int = 30) -> pd.DataFrame:
    """
    Read the last N days of hourly adjusted close prices for a ticker.
    Assumes BigQuery table schema matches create_tables.sql:
      ts TIMESTAMP, adj_close FLOAT64, ticker STRING, ...
    """
    query = f"""
      SELECT ts, adj_close
      FROM `{T_H}`
      WHERE ticker = @t
        AND ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @d DAY)
      ORDER BY ts
    """
    job = bq.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("t", "STRING", ticker),
                bigquery.ScalarQueryParameter("d", "INT64", days),
            ]
        ),
    )
    return job.result().to_dataframe()


def fit_forecast(df: pd.DataFrame, steps: int, seasonal: str) -> pd.DataFrame:
    """
    Fit a simple SARIMAX model and forecast.
    df must have index as datetime and column y.
    """
    if df.empty:
        return pd.DataFrame(columns=["ts", "yhat", "yhat_lower", "yhat_upper"])

    season_len = 24 if seasonal == "H" else 7
    model = SARIMAX(
        df["y"],
        order=(1, 1, 1),
        seasonal_order=(0, 1, 1, season_len),
        enforce_stationarity=False,
        enforce_invertibility=False,
    ).fit(disp=False)

    forecast = model.get_forecast(steps=steps)
    ci = forecast.conf_int(alpha=0.2)

    out = pd.DataFrame(
        {
            "ts": pd.date_range(
                df.index[-1],
                periods=steps + 1,
                freq=("H" if seasonal == "H" else "D"),
                inclusive="neither",
            ),
            "yhat": forecast.predicted_mean,
            "yhat_lower": ci.iloc[:, 0],
            "yhat_upper": ci.iloc[:, 1],
        }
    )
    return out


@app.post("/")
def run():
    """
    Entry point (Pub/Sub style envelope or bare JSON).
    Writes:
      - hourly-based forecasts to forecast_short
      - daily-based forecasts to forecast_long
    """
    env = request.get_json(silent=True) or {}
    msg = env.get("message", {})
    data_raw = msg.get("data")

    if data_raw:
        # Pub/Sub base64 wrapped
        data = json.loads(base64.b64decode(data_raw).decode("utf-8"))
    else:
        # Direct JSON POST
        data = env

    tickers = data.get("tickers", ["HDFCBANK.NS"])
    short_h = int(data.get("short_horizon_hours", 24))
    long_d = int(data.get("long_horizon_days", 90))
    model_name = data.get("model", "sarimax")
    now = datetime.now(timezone.utc)

    for t in tickers:
        # --- Short-term (hourly) forecast -> forecast_short ---
        hist_h = read_hourly(t, days=30)
        if not hist_h.empty:
            x = (
                hist_h.rename(columns={"ts": "ds", "adj_close": "y"})
                .set_index("ds")
                .asfreq("H")
            )

            short_fc = fit_forecast(x, short_h, "H")
            if not short_fc.empty:
                short_fc["ticker"] = t
                short_fc["ds"] = short_fc["ts"].dt.date  # DATE for ds
                short_fc["horizon"] = f"{short_h}h"
                short_fc["model"] = model_name
                short_fc["run_ts"] = now

                # Match forecast_short schema exactly
                to_load = short_fc[
                    ["ticker", "ds", "yhat", "yhat_lower", "yhat_upper", "horizon", "model", "run_ts"]
                ]
                bq.load_table_from_dataframe(to_load, OUT_S).result()

        # --- Long-term (daily) forecast -> forecast_long ---
        # Aggregate hourly to daily
        daily_df = bq.query(
            f"""
            SELECT
              DATE(ts) AS ds,
              ANY_VALUE(adj_close) AS adj_close
            FROM `{T_H}`
            WHERE ticker = @t
            GROUP BY ds
            HAVING ds >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
            ORDER BY ds
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("t", "STRING", t),
                ]
            ),
        ).result().to_dataframe()

        if not daily_df.empty:
            x_d = (
                daily_df.rename(columns={"ds": "ds", "adj_close": "y"})
                .set_index("ds")
                .asfreq("D")
            )
            long_fc = fit_forecast(x_d, long_d, "D")
            if not long_fc.empty:
                long_fc["ticker"] = t
                long_fc["ds"] = long_fc["ts"].dt.date  # DATE for ds
                long_fc["horizon"] = f"{long_d}d"
                long_fc["model"] = model_name
                long_fc["run_ts"] = now

                to_load_l = long_fc[
                    ["ticker", "ds", "yhat", "yhat_lower", "yhat_upper", "horizon", "model", "run_ts"]
                ]
                bq.load_table_from_dataframe(to_load_l, OUT_L).result()

    return jsonify({"status": "ok"}), 200


@app.get("/healthz")
def health():
    return ("ok", 200)


