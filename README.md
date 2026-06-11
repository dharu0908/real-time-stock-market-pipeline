# 📈 Real-Time Stock Market Data Pipeline (Kafka + Spark + PostgreSQL)

## 🚀 Overview

This project is a **real-time streaming data engineering pipeline** that simulates stock market tick data, processes it through a **Medallion Architecture (Bronze → Silver → Gold)** using **Apache Kafka and Apache Spark Structured Streaming**, and generates real-time financial analytics stored in **PostgreSQL and Parquet storage**.

The system is designed to mimic a **production-grade low-latency trading data pipeline** used in financial systems.

---

## 🏗️ Architecture

Stock Tick Generator (Python)
        │
        ▼
Kafka Topic (Real-Time Event Streaming)
        │
        ▼
Bronze Layer (Raw Ingestion - Spark Streaming)
        │
        ├── Valid Events → Bronze Parquet Storage
        └── Invalid Events → Dead Letter Queue (DLQ)
        │
        ▼
Silver Layer (Cleaned & Structured Data)
        │
        ▼
Gold Layer (Business Aggregations - OHLCV + Analytics)
        │
        ├── PostgreSQL (Analytics Tables)
        └── Parquet Storage (Historical Data)

---

## ⚙️ Tech Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker
- Parquet Storage
- Confluent Kafka Python Client

---

## 📂 Project Structure

producer/ -> Generates real-time stock ticks
consumer/ -> Bronze layer ingestion from Kafka
transformation/ -> Silver & Gold transformations
serving/ -> Analytics layer
config/ -> Configurations
docker/ -> Docker setup

---

## 🔄 Data Flow

Producer → Kafka → Bronze → Silver → Gold → PostgreSQL + Parquet

---

## 📊 Gold Layer Metrics

- OHLCV Candles (Open, High, Low, Close, Volume)
- VWAP
- Tick Count
- Spread Metrics
- Top Movers (% Change)

---

## ⚡ Features

- Real-time streaming pipeline
- Medallion architecture
- Kafka ingestion
- Spark Structured Streaming
- Dead Letter Queue handling
- Checkpointing
- PostgreSQL analytics layer

---

## 🐳 How to Run

docker-compose up -d
python producer/producer.py
python consumer/bronze_stream.py
python transformation/silver.py
python transformation/gold.py

---

## 🚀 Future Improvements

- Airflow orchestration
- AWS deployment (S3 + Glue + MSK)
- Grafana dashboard
- dbt transformations
- ML price prediction

---

## 👨‍💻 Author

Dharmik Patel
