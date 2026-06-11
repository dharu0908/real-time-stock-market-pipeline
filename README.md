# 🚀 Real-Time Stock Streaming Pipeline (Kafka + Spark + PostgreSQL)

A production-style end-to-end real-time data engineering pipeline using Kafka, Spark Structured Streaming, and PostgreSQL with Python and Docker.

---

## 📌 Architecture

```
Producer → Kafka → Spark Streaming → PostgreSQL → Analytics Layer
```

---

## 🔁 Data Flow

### 1. Producer (Python)
- Generates real-time stock OHLCV data
- Publishes events to Kafka topic

### 2. Kafka
- Acts as streaming message broker
- Buffers real-time events

### 3. Spark Streaming
- Consumes Kafka stream
- Performs transformations
- Builds Silver & Gold layers

### 4. PostgreSQL
- Stores structured Gold (OHLCV candles) data
- Used for analytics queries

### 5. Analytics Layer
- Runs SQL-based insights using Spark
- Computes:
  - Latest candle per ticker
  - Price performance
  - Volume leaders
  - Volatility metrics

---

## 🧱 Project Structure

```
spark_streaming_my_own/
│
├── config/
├── producer/
├── consumer/
├── transformation/
├── serving/
├── docker/
│   └── docker-compose.yml
├── checkpoints/
├── data/
├── .gitignore
├── requirements.txt
└── README.md
```

---

## ⚙️ Docker Setup

### Start all services
```bash
cd docker
docker-compose up -d
```

### Stop services
```bash
docker-compose down
```

### Stop + remove volumes
```bash
docker-compose down -v
```

---

## 🧩 Services

| Service    | Description        | Port |
|------------|--------------------|------|
| Kafka      | Message broker     | 9092 |
| Zookeeper  | Coordination       | 2181 |
| Kafka UI   | Topic viewer       | 8080 |
| PostgreSQL | Database           | 5432 |
| pgAdmin    | DB UI              | 5050 |

---

## 📊 Analytics Features

- Latest candles per ticker
- Price performance (% change)
- Volume leaders
- Volatility metrics

---

## 🧠 Tech Stack

- Kafka
- Spark Structured Streaming
- PostgreSQL
- Python
- Docker

---

## 🚀 How to Run

1. Start infra:
```bash
cd docker
docker-compose up -d
```

2. Run producer:
```bash
python producer/producer.py
```

3. Run streaming:
```bash
python transformation/streaming_job.py
```

4. Run analytics:
```bash
python serving/analytics.py
```

---

## 📦 Git Ignore

```
.venv/
__pycache__/
checkpoints/
data/
*.pyc
*.log
```

---

## 👨‍💻 Author

Dharmik Patel
