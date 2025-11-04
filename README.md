# Healthcare-ETL-Data-Pipeline

## Project Overview

This project automates the end-to-end data engineering workflow for healthcare data processing using Apache Airflow and Docker.
It extracts raw patient admission records, validates and cleans them, computes key healthcare KPIs (e.g., readmission rate, average length of stay), and stores both the cleaned data and KPI metrics in Amazon S3 for downstream analytics and reporting.
The pipeline simulates a production-grade healthcare data workflow — fully automated, validated, and containerized.

## Tech Stack

| Component                | Tool                           |
| ------------------------ | ------------------------------ |
| Orchestration            | **Apache Airflow**             |
| Containerization         | **Docker**                     |
| Programming Language     | **Python 3.9+**                |
| Cloud Storage            | **Amazon S3**                  |
| Logging                  | **Python Logging Module**      |
| Scheduling               | **Airflow DAG (TaskFlow API)** |
