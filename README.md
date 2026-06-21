# Real-Time Stock Market Data Pipeline

## Overview

Financial applications require the ability to process and analyze market data in real time. Traditional batch pipelines introduce latency, making them unsuitable for monitoring rapidly changing stock prices and trading activity.

This project builds an end-to-end real-time data platform using Apache Kafka, Spark Structured Streaming, Delta Lake, PostgreSQL, and Streamlit.

The platform ingests simulated stock market data, processes it through a Medallion Architecture (Bronze → Silver → Gold), calculates financial metrics, and serves analytics through an interactive dashboard.

---

## Business Problem

Financial systems generate thousands of market events every second.

Organizations need to:

- Process stock market events in real time
- Maintain data quality and reliability
- Calculate trading metrics continuously
- Deliver low-latency analytics
- Support scalable downstream applications

This project demonstrates how modern streaming technologies can be used to build a production-style stock market analytics platform.

---

## Architecture

![Architecture](screenshots/architecture.png)

### Data Flow

```text
Stock Tick Generator
         │
         ▼
    Apache Kafka
         │
         ▼
 Spark Structured Streaming
         │
 ┌───────┼────────┐
 ▼       ▼        ▼
Bronze  Silver   Gold
         │
         ▼
    PostgreSQL
         │
         ▼
      Streamlit
```

---

## Technology Stack

| Layer | Technology |
|---------|------------|
| Programming | Python |
| Streaming Platform | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Processing Framework | PySpark |
| Storage Format | Delta Lake |
| Database | PostgreSQL |
| Dashboard | Streamlit |
| Containerization | Docker |
| Monitoring | Kafka UI |
| CI/CD | GitHub Actions |
| Version Control | Git & GitHub |

---

## Data Pipeline

### Bronze Layer

The Bronze layer ingests raw stock tick events from Kafka and stores them in Delta Lake.

Responsibilities:

- Schema validation
- Raw event persistence
- Fault tolerance through checkpointing
- Data quality validation

Example fields:

```text
event_id
ticker
price
bid
ask
volume
event_time
```

---

### Silver Layer

The Silver layer enriches and standardizes market events.

Calculated metrics:

- Spread
- Spread Percentage
- Notional Value
- Price Bucket Classification

This layer prepares data for analytical consumption.

---

### Gold Layer

The Gold layer generates business-ready analytics.

Metrics include:

- OHLCV Candles
- VWAP (Volume Weighted Average Price)
- Average Spread Percentage
- Price Range
- Top Movers Analysis

These datasets are loaded into PostgreSQL for serving and visualization.

---

## Dashboard

A Streamlit dashboard was built on top of the PostgreSQL serving layer to provide real-time market analytics.

Features:

- Stock selection by ticker
- KPI monitoring
- Price trend visualization
- VWAP tracking
- Volume analysis
- OHLCV reporting

### Dashboard Overview

![Dashboard Overview](screenshots/streamlit_1.png)

### OHLCV Analytics

![OHLCV Analytics](screenshots/streamlit_2.png)

---

## Key Features

### Real-Time Processing

- Kafka-based event streaming
- Spark Structured Streaming
- Continuous micro-batch processing

### Delta Lake

- Bronze, Silver, and Gold storage layers
- ACID transactions
- Reliable data persistence

### Fault Tolerance

- Streaming checkpoints
- Watermarking
- Recovery support

### Data Quality

- Schema validation
- Event standardization
- Deduplication handling

### DevOps

- Dockerized infrastructure
- GitHub Actions CI/CD pipeline
- Automated validation workflows

---

## Pipeline Screenshots

### Kafka Infrastructure

![Kafka Topics](screenshots/kafka-topics-list.png)

### Kafka Messages

![Kafka Messages](screenshots/kafka-messages.png)

### Producer

![Producer](screenshots/kafka-producer-running.png)

### Gold Layer Analytics

![Gold Layer](screenshots/postgres-gold-layer.png)

### Docker Services

![Docker Containers](screenshots/docker-containers.png)

---

## Project Structure

```text
real-time-stock-market-pipeline/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── bronze_stream.py
│
├── transformation/
│   ├── silver.py
│   └── gold.py
│
├── dashboard/
│   └── app.py
│
├── serving/
│
├── docker/
│   ├── docker-compose.yml
│   └── init.sql
│
├── config/
│   └── config.py
│
├── screenshots/
│
├── .github/
│   └── workflows/
│
└── README.md
```

---

## Project Outcomes

- Built an end-to-end real-time stock market analytics platform.
- Implemented a Medallion Architecture using Delta Lake.
- Processed streaming stock events using Kafka and Spark Structured Streaming.
- Calculated financial metrics including OHLCV and VWAP.
- Developed a real-time Streamlit dashboard for analytics consumption.
- Implemented Docker-based deployment and infrastructure management.
- Added GitHub Actions for automated validation and CI/CD.

---

## Skills Demonstrated

- Python
- Apache Kafka
- Apache Spark
- PySpark
- Structured Streaming
- Delta Lake
- PostgreSQL
- Docker
- Streamlit
- GitHub Actions
- Real-Time Data Processing
- Data Engineering

---
