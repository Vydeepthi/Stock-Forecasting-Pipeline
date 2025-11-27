# 🚀 Stock Forecasting Pipeline — End-to-End (BigQuery + SARIMA + Prophet)

This project implements a complete **stock price forecasting pipeline** using:

- **Yahoo Finance** for data ingestion  
- **Cloud Run + Cloud Scheduler** for automated extraction
- **BigQuery** for storage
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
    ├── extractor/
    │   ├── main.py
    │   ├── Dockerfile
    │   ├── requirements.txt
    │
    └── forecaster/
        ├── main.py
        ├── Dockerfile
        ├── requirements.txt
│
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
## EXTRACTOR - AUTOMATED DATA INGESTION
Runs every hour using Cloud Scheduler --> CLOUD Run
**Build&Deploy Extractor to Cloud Run**
```bash
cd ~/stock-forecasting-pipeline
gcloud builds submit --config=cloudbuild-extractor.yaml .
```
**Then Deploy**
```bash
gcloud run deploy extractor-service \
  --region=us-central1 \
  --source=services/extractor \
  --allow-unauthenticated
```
**Copy Cloud Run URL(looks like):**
https://extractor-service-XXXX-uc.a.run.app

## SCHEDULING THE EXTRACTOR
**Create Cloud Scheduler Job:**
```bash
gcloud scheduler jobs create http extractor-hourly \
  --schedule="0 * * * *" \
  --time-zone="Asia/Kolkata" \
  --uri="YOUR_EXTRACTOR_URL" \
  --http-method=GET \
  --location=us-central1
```
**Check logs**
```bash
gcloud run services logs read extractor-service --region=us-central1
```

## FORECATER - SHORT-TERM(SARIMA) & LONG-TERM(PROPHET)
**Run locally inside Cloud Shell:**
```bash
cd ~/stock-forecasting-pipeline
export PROJECT_ID=deft-gearbox-410821
export GOOGLE_CLOUD_PROJECT=$PROJECT_ID

python services/forecaster/main.py
```
**Output Produced in BigQuery:**
market.forecast_short --> 24-hour SARIMA forecast
market.forecast_lond --> 90-day Prophet forecast
market.eval_metrics --> model performance metrics

## BACKFILL
**To load all hostorical prices**
```bash
python backfill_prices.py
```
This prepares **10 years of daily data** for Prophet + hourly data for Sarima

## How to Run Full Pipeline from scratch

```bash
cd ~/stock-forecasting-pipeline
gcloud auth login
gcloud config set project deft-gearbox-410821

# Create dataset/tables
bq --location=US mk -d market
# (run the CREATE TABLE commands above)

# Run backfill
python backfill_prices.py

# Deploy extractor
gcloud builds submit --config=cloudbuild-extractor.yaml .
gcloud run deploy extractor-service --region=us-central1 --source=services/extractor --allow-unauthenticated

# Create hourly scheduler
gcloud scheduler jobs create http extractor-hourly \
  --schedule="0 * * * *" \
  --time-zone="Asia/Kolkata" \
  --uri="EXTRACTOR_URL" \
  --http-method=GET \
  --location=us-central1

# Run forecasting
python services/forecaster/main.py
```


## Deployment
**Forecaster as Cloud Run Service**
```bash
gcloud run deploy forecaster-service \
  --region=us-central1 \
  --source=services/forecaster \
  --allow-unauthenticated
```

Then schedule daily forecast jobs with Cloud Scheduler.
