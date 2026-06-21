# Real-Time Stock Market Data Pipeline

## Overview

Financial applications require the ability to process and analyze market data in real time. Traditional batch pipelines introduce latency, making them unsuitable for monitoring rapidly changing stock prices and trading activity.

This project builds an end-to-end real-time data platform using Apache Kafka, Spark Structured Streaming, Delta Lake, PostgreSQL, Docker, and Streamlit.

The platform ingests simulated stock market events, processes them through a Medallion Architecture (Bronze → Silver → Gold), generates financial analytics, and serves insights through an interactive dashboard.

---

## Business Problem

Modern trading and financial systems generate thousands of events every second.

Organizations need to:

- Process stock market events in real time
- Maintain data quality and reliability
- Calculate trading metrics continuously
- Deliver low-latency analytics
- Support scalable downstream applications

This project demonstrates how modern streaming technologies can be used to build a production-style stock market analytics platform.

---

## Architecture

The pipeline follows a real-time Medallion Architecture with a dedicated serving layer.

![Architecture](Screenshots/diagram.png)

### Data Flow

```text
Stock Tick Producer
        │
        ▼
Apache Kafka
(stock_ticks topic)
        │
        ▼
Spark Structured Streaming
        │
        ▼
Bronze Layer - Delta Lake
Raw validated tick events
        │
        ▼
Silver Layer - Delta Lake
Cleaned + enriched ticks
spread, spread %, notional value
        │
        ▼
Gold Layer - Delta Lake
OHLCV, VWAP, Top Movers
        │
        ▼
Serving Layer - Spark SQL
Latest candles, price performance,
volume leaders, volatility analysis
        │
        ▼
PostgreSQL
Analytics serving tables
        │
        ▼
Streamlit Dashboard
Real-time stock insights
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

## Serving Layer

A dedicated serving layer was implemented using Spark SQL to generate business-ready analytical datasets from the Gold layer.

### Available Analytics

#### Latest Candles

Returns the latest OHLCV candle for each stock ticker.

#### Price Performance

Calculates:

- Day Open
- Day Close
- Percentage Change
- Daily High
- Daily Low
- Total Volume

#### Volume Leaders

Identifies the most actively traded stocks based on cumulative trading volume.

#### Volatility Analysis

Calculates:

- Average Volatility
- Maximum Volatility
- Green Candle Count
- Red Candle Count

These analytical datasets are then loaded into PostgreSQL and consumed by the Streamlit dashboard.

---

## Dashboard

A Streamlit dashboard was built on top of the PostgreSQL serving layer to provide real-time market analytics.

Features include:

- Stock selection by ticker
- KPI monitoring
- Price trend visualization
- VWAP tracking
- Volume analysis
- OHLCV reporting

---

## Screenshots

### Infrastructure Architecture

![Architecture](Screenshots/diagram.png)

---

### Docker Infrastructure

Docker containers running Kafka, PostgreSQL, Kafka UI, and supporting services.

![Docker Infrastructure](Screenshots/docker-containers.png)

---

### Kafka Producer

Simulated stock market events continuously generated and published to Kafka.

![Kafka Producer](Screenshots/kafka-producer-running.png)

---

### Kafka Topics

Kafka topic configuration and metadata.

![Kafka Topics](Screenshots/kafka-topics-list.png)

![Kafka Topic Overview](Screenshots/kafka-topic-overview.png)

---

### Live Streaming Events

Real-time stock market messages flowing through Kafka.

![Kafka Messages](Screenshots/kafka-messages.png)

---

### Pipeline Execution

Spark Structured Streaming processing Bronze, Silver, and Gold layers.

![Pipeline Execution](Screenshots/gold-batch-log.png)

---

### Gold Layer Analytics

Aggregated analytics and OHLCV data stored in PostgreSQL.

![Gold Layer Analytics](Screenshots/postgres-gold-layer.png)

---

### Streamlit Dashboard

#### Dashboard Overview

![Dashboard Overview](Screenshots/streamlit_1.png)

#### OHLCV Analytics

![OHLCV Analytics](Screenshots/streamlit_2.png)

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
├── serving/
│   └── analytics.py
│
├── dashboard/
│   └── app.py
│
├── docker/
│   ├── docker-compose.yml
│   └── init.sql
│
├── config/
│   └── config.py
│
├── Screenshots/
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
- Implemented a dedicated serving layer using Spark SQL.
- Loaded analytical datasets into PostgreSQL for downstream consumption.
- Added GitHub Actions for automated validation and CI/CD.
- Containerized the entire platform using Docker.

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
- Data Modeling
- Financial Analytics

---

## Future Enhancements

- Airflow or Dagster orchestration
- Cloud deployment (AWS MSK, EMR, Redshift)
- Real-time alerting system
- ML-based stock movement prediction
- Kubernetes deployment
- REST API serving layer

---

## Author

Dharmik Patel

Portfolio: https://www.ptldharmik.com
