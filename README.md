Lab 2 – End-to-End Data Analytics Pipeline

Snowflake • Airflow • dbt • Preset (Superset)


LAB 2

👥 Team Members

Elsa Rose – GitHub Link

Kruthika Virupakshappa – GitHub Link

Lab Group: 9

Our team consists of two applied analytics and data engineering students who collaborated to design a scalable stock price analytics system. Our combined skills in Python, cloud-based pipelines, Airflow, Snowflake, dbt, and BI visualization allowed us to build a fully automated ELT pipeline with scheduled workflows and analytical dashboards.

📌 Project Overview

This lab extends the foundational work from Lab 1 into a complete, production-style ELT data pipeline.
The system incorporates:

Airflow for ETL orchestration

Snowflake as the cloud data warehouse

dbt for modular SQL transformations, tests, and snapshots

Preset/Superset for business intelligence dashboards

The final pipeline ingests stock price data, transforms it into analytical tables, schedules automated workflows, and visualizes insights from financial indicators.

🎯 Problem Statement

The goal is to build a scheduled, automated analytics pipeline to process multi-year historical stock data.

The system must:

Ingest raw stock prices from API → Snowflake

Transform data using dbt into analytical models (SMA20, rolling sums, daily price changes, etc.)

Orchestrate ETL + ELT using Apache Airflow

Visualize insights through an interactive dashboard (Preset)

The outcome is a scalable, reusable, version-controlled data analytics workflow.

✔️ Functional Requirements

Load raw stock price data from an API into Snowflake using Airflow

Transform data via dbt models (staging, mart, snapshots)

Trigger dbt (run, test, snapshot) from Airflow

Build BI dashboards with Preset

Ensure ETL idempotency using SQL MERGE & transactions

🔧 Technical Specifications
Component	Technology
Cloud Warehouse	Snowflake
Orchestration	Apache Airflow
Transformations	dbt
Visualization	Preset / Superset
Languages	SQL, Python, YAML
Version Control	GitHub
📊 Data Specifications

Dataset: Historical stock prices (daily)

Fields: ticker, date, open, close, high, low, volume

Symbols used: NVDA, TSLA

🏗️ Architecture

Pipeline Flow:

API → Airflow (ETL) → Snowflake RAW → Airflow (ELT) → dbt Models → Snowflake MART → BI Dashboard


Major Components:

1️⃣ Data Source – Alpha Vantage / yFinance

Provides daily stock price data.

2️⃣ Airflow ETL DAG

Creates Snowflake tables

Extracts API data

Loads into RAW layer using SQL MERGE for idempotency

3️⃣ Snowflake Data Warehouse

Stores raw + transformed data

Provides compute for dbt modeling

4️⃣ Airflow ELT DAG

Runs dbt commands in sequence:

dbt snapshot

dbt run

dbt test

5️⃣ Preset Dashboard

Visualizes stock price trends, momentum, and daily changes.

🛠️ Airflow DAGs
ETL DAG: stock_price_etl_vantage

Runs daily at 2:30 AM

Creates or replaces Snowflake table

Fetches stock prices for NVDA & TSLA

Loads data using MERGE (update or insert)

Key tasks:

Task	Description
create_table_task	Initializes Snowflake RAW table
download_stock_data	Fetches API data for each symbol
load_records	Loads/updates Snowflake table
ELT DAG: BuildELT_dbt

Runs daily at 2:45 AM

Runs dbt workflows in order:

dbt snapshot → dbt run → dbt test


Ensures transformations and tests run after ETL completes

Uses BashOperator to execute dbt CLI commands

🧮 dbt Transformations
Key Calculations in the mart model (fct_stock_indicators.sql)

Previous Close

20-day Simple Moving Average (SMA20)

50-day Rolling Sum

Daily Price Change

These metrics support trend analysis and momentum tracking.

dbt Tests Validate:

symbol is never NULL

date is never NULL

close is not NULL

volume is valid

📊 BI Dashboards (Preset)

Dashboards were built for NVDA and TSLA, containing:

1️⃣ 50-Day Momentum Chart

Shows upward or downward momentum based on rolling indicators.

2️⃣ Price Trend Analysis

Displays long-term movement in closing prices.

3️⃣ Daily Price Movements

Shows day-over-day gains or losses.

Observations:

NVDA displays strong consistent growth and stable upward momentum

TSLA shows higher price volatility with sharp momentum swings

✅ Conclusion

This lab successfully demonstrates a fully automated modern data engineering pipeline, integrating:

Structured ingestion (Airflow ETL)

Modular transformations (dbt)

Cloud warehousing (Snowflake)

Automated workflow scheduling (Airflow)

Intuitive analytics (Preset/Superset)

The system provides a scalable foundation for real-world data pipelines and financial analytics.
