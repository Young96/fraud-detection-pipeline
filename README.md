# Credit Card Fraud Detection - End-to-End Data Pipeline

An end-to-end data engineering pipeline that processes credit card transaction data to detect fraud patterns. Built using PySpark, Spark SQL, and Apache Airflow following the modern Medallion Architecture (Bronze, Silver, Gold layers).

---

## Project Overview

This project implements a production-ready data pipeline that:
- Ingests raw credit card transaction data
- Cleans and enriches it with derived features
- Aggregates business-ready insights for fraud detection
- Is orchestrated via Apache Airflow for daily automated runs

**Dataset:** Credit Card Fraud Detection by ULB (Kaggle)
**Records:** 284,807 transactions over 2 days
**Fraud Cases:** 492 (0.172% - highly imbalanced)

---

## Architecture - Medallion Pattern

Source CSV --> Bronze (Raw) --> Silver (Cleaned) --> Gold (Aggregated) --> Spark SQL Analytics

Orchestrated by Apache Airflow (Daily at 2 AM)

### Bronze Layer (Raw Data)
- Stores raw data exactly as received from source
- No transformations - acts as source of truth
- Format: Parquet (faster reads, compressed)

### Silver Layer (Cleaned and Enriched)
- Removes duplicates (1,081 duplicates removed)
- Adds time-based features: hour_of_day, day_number
- Categorizes amounts: Small, Medium, Large, XLarge
- Partitioned by day_number for query optimization

### Gold Layer (Business-Ready Aggregations)
- Hourly Fraud Analysis - Fraud patterns by hour of day
- Daily Summary - Day-over-day KPIs
- Category Analysis - Risk analysis by amount range

---

## Key Insights Discovered

| Insight | Finding |
|---------|---------|
| Peak Fraud Hour | 2 AM with 1.451% fraud rate |
| Night vs Day | Night fraud rate is 2x higher than Day |
| Highest Money Loss | Large category (100-1000 USD) - 38,497 USD loss |
| Highest Fraud Rate | XLarge category (over 1000 USD) - 0.294% |
| Day Trend | Day 1: 272 frauds, Day 2: 201 frauds |

---

## Tech Stack

- PySpark - Distributed data processing
- Spark SQL - SQL-based analytics layer
- Apache Airflow - Workflow orchestration
- Parquet - Columnar storage format
- Google Colab - Development environment
- Python 3.x - Programming language

---

## Project Structure

- fraud-detection-pipeline/
  - notebooks/fraud_detection_pipeline.ipynb (Main pipeline notebook)
  - dags/fraud_detection_pipeline_dag.py (Airflow DAG)
  - data/bronze/ (Raw data storage)
  - data/silver/ (Cleaned data)
  - data/gold/ (Aggregated tables - hourly_fraud, daily_summary, category_analysis)
  - README.md

---

## Pipeline Stages

### Stage 1: Data Ingestion (Bronze)
- Loads raw CSV with 284,807 transactions
- Generates data quality report
- Saves as Parquet format

### Stage 2: Data Cleaning (Silver)
- Removes 1,081 duplicate rows
- Extracts hour_of_day and day_number from Time column
- Categorizes Amount into 4 buckets
- Saves with partitioning by day_number

### Stage 3: Business Aggregations (Gold)
- Creates 3 business-ready tables
- Hourly fraud trends
- Daily summary metrics
- Amount category risk analysis

### Stage 4: Analytics (Spark SQL)
- Top fraud transactions identification
- Day vs Night fraud comparison
- Cumulative fraud trend analysis with window functions

### Stage 5: Orchestration (Airflow)
- Daily scheduled DAG at 2 AM
- 5 sequential tasks with retries and alerts
- Email notifications on failure
- Data quality validation step

---

## Airflow DAG Flow

bronze_ingestion --> silver_processing --> gold_aggregation --> data_quality_check --> send_notification

Each task includes:
- 3 automatic retries on failure
- 5 minute retry delay
- 2 hour execution timeout
- Email alerts on failure

---

## Data Quality Checks

The pipeline includes built-in data quality validations:
- NULL value detection across all columns
- Duplicate row identification and removal
- Class distribution verification
- Row count validation between layers

---

## How to Run

### Prerequisites
- Python 3.8+
- PySpark
- Apache Airflow (for orchestration)
- Kaggle API credentials

### Setup Steps
1. Clone this repository
2. Install dependencies: pip install pyspark
3. Configure Kaggle API credentials
4. Open notebook in Google Colab or Jupyter
5. Run cells sequentially

### For Production Deployment
1. Copy DAG file to Airflow's dags folder
2. Update file paths to production locations (S3/HDFS)
3. Configure email alerts in Airflow connections
4. Enable DAG in Airflow UI

---

## Future Enhancements

- Integrate with Kafka for real-time streaming
- Add machine learning model for fraud prediction
- Implement Delta Lake for ACID transactions
- Add Grafana dashboards for monitoring
- Migrate to AWS EMR for production scale
- Implement SCD Type 2 for slowly changing dimensions

---

## Learning Outcomes

This project demonstrates proficiency in:
- Designing layered data architectures (Medallion pattern)
- Building scalable PySpark pipelines
- Writing optimized Spark SQL queries
- Orchestrating workflows with Airflow
- Working with imbalanced datasets
- Implementing data quality checks
- Following Data Engineering best practices

---

## Contact

Author: Tarun Chourasia
GitHub: Young96
Project Link: github.com/Young96/fraud-detection-pipeline

---

## Acknowledgments

- Dataset: ULB Machine Learning Group (Universite Libre de Bruxelles)
- Kaggle for hosting the dataset
- Apache Spark and Airflow communities
