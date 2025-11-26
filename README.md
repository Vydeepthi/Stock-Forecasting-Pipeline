# 🚀 Stock Forecasting Pipeline — End-to-End (BigQuery + SARIMA + Prophet)

This project implements a complete **stock price forecasting pipeline** using:

- **Yahoo Finance** for data ingestion  
- **Google Cloud BigQuery** for scalable storage  
- **SARIMA** for short-term (24-hour) forecasts  
- **Prophet** for long-term (90-day) forecasts  
- **Python scripts** for automation and computation  

All components have been tested end-to-end inside **Google Cloud Shell**.

---

##  Project Structure

    stock-forecasting-pipeline/
    │
    ├── backfill_prices.py
    ├── requirements.txt
    │
    └── services/
        └── forecaster/
            ├── main.py
            ├── Dockerfile
   └── cloudbuild-extractor.yaml

## Installation & Environment Setup
Follow these steps to set up the environment locally or inside Google Cloud Shell.

1. Clone the Repository
```bash
git clone https://github.com/<your-username>/stock-forecasting-pipeline.git
cd stock-forecasting-pipeline
```
2. Install Dependencies
```bash
pip install -r requirements.txt
```

## Google Cloud Setup
1. Set Active Project
```bash
gcloud config set project [YourProjectID]
```
2. Authenticate User
```bash
gcloud auth login
gcloud auth application-default login

```
3. Create Big Query Dataset
```bash
bq --location=US mk -d market
```

## BigQuery Table Setup
Run these SQL commands in Cloud Shell: 

Hourly Prices Table
```bash
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `deft-gearbox-410821.market.prices_hourly` (
  ts TIMESTAMP,
  ticker STRING,
  adj_close FLOAT64
);'
```

Daily Prices Table
```bash
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `deft-gearbox-410821.market.prices_daily` (
  ts TIMESTAMP,
  ticker STRING,
  adj_close FLOAT64
);'
```

Short Term Forecast Output Table
```bash
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `deft-gearbox-410821.market.forecast_short` (
  ts TIMESTAMP,
  ticker STRING,
  forecast FLOAT64,
  horizon STRING,
  created_ts TIMESTAMP
);'
```

Long Term Forecast Output Table
```bash
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `deft-gearbox-410821.market.forecast_long` (
  ds TIMESTAMP,
  ticker STRING,
  yhat FLOAT64,
  yhat_lower FLOAT64,
  yhat_upper FLOAT64,
  created_ts TIMESTAMP
);'
```
Evaluation Metrics Table

```bash
bq query --use_legacy_sql=false '
CREATE OR REPLACE TABLE `deft-gearbox-410821.market.eval_metrics` (
  model STRING,
  horizon STRING,
  rmse FLOAT64,
  mae FLOAT64,
  mape FLOAT64,
  mean_price FLOAT64,
  benchmark_5pct_mean FLOAT64,
  rmse_vs_5pct_mean_pass BOOL,
  ts TIMESTAMP
);'
```

## Running the Pipeline
**Backfill Yahoo Finance Data**
Loads historical stock data into BigQuery:
```bash
python backfill_prices.py
```
**RUN the Forecaster**
```bash
python services/forecaster/main.py
```

