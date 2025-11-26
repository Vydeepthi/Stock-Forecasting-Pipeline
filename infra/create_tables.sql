-- Create tables for the pipeline
CREATE SCHEMA IF NOT EXISTS `{{PROJECT}}`.market;

CREATE TABLE IF NOT EXISTS `{{PROJECT}}`.market.prices_hourly (
  ts TIMESTAMP,
  ticker STRING,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  adj_close FLOAT64,
  volume INT64,
  src STRING,
  load_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{PROJECT}}`.market.prices_daily (
  ts TIMESTAMP, -- market close date
  ticker STRING,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  close FLOAT64,
  adj_close FLOAT64,
  volume INT64,
  load_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{PROJECT}}`.market.forecast_short (
  ticker STRING,
  ds DATE,
  yhat FLOAT64,
  yhat_lower FLOAT64,
  yhat_upper FLOAT64,
  horizon STRING,
  model STRING,
  run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{PROJECT}}`.market.forecast_long (
  ticker STRING,
  ds DATE,
  yhat FLOAT64,
  yhat_lower FLOAT64,
  yhat_upper FLOAT64,
  horizon STRING,
  model STRING,
  run_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{PROJECT}}`.market.eval_metrics (
  run_ts TIMESTAMP,
  ticker STRING,
  metric STRING,
  value FLOAT64,
  details STRING
);
