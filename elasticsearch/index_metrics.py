"""
Elasticsearch Indexing — Stock Market Metrics

Reads Gold-layer stock-market analytics from PostgreSQL and indexes them into
Elasticsearch for fast querying, monitoring, and dashboard search.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

import psycopg2
from elasticsearch import Elasticsearch, helpers


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres-service")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dharmik")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch-service")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", "9200"))
INDEX_INTERVAL_SECONDS = int(os.getenv("INDEX_INTERVAL_SECONDS", "0"))

OHLCV_INDEX = "stock_ohlcv"
VWAP_INDEX = "stock_vwap"
VOLATILITY_INDEX = "stock_volatility"


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def get_es_client() -> Elasticsearch:
    return Elasticsearch(f"http://{ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}")


def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# ---------------------------------------------------------------------------
# Index definitions
# ---------------------------------------------------------------------------

def create_indices(es: Elasticsearch) -> None:
    """Create Elasticsearch indices with mappings used by the dashboard."""

    mappings = {
        OHLCV_INDEX: {
            "mappings": {
                "properties": {
                    "ticker": {"type": "keyword"},
                    "window_start": {"type": "date"},
                    "window_end": {"type": "date"},
                    "open": {"type": "float"},
                    "high": {"type": "float"},
                    "low": {"type": "float"},
                    "close": {"type": "float"},
                    "volume": {"type": "long"},
                    "indexed_at": {"type": "date"},
                }
            }
        },
        VWAP_INDEX: {
            "mappings": {
                "properties": {
                    "ticker": {"type": "keyword"},
                    "window_start": {"type": "date"},
                    "vwap": {"type": "float"},
                    "total_volume": {"type": "long"},
                    "indexed_at": {"type": "date"},
                }
            }
        },
        VOLATILITY_INDEX: {
            "mappings": {
                "properties": {
                    "ticker": {"type": "keyword"},
                    "avg_volatility": {"type": "float"},
                    "max_volatility": {"type": "float"},
                    "green_candles": {"type": "integer"},
                    "red_candles": {"type": "integer"},
                    "indexed_at": {"type": "date"},
                }
            }
        },
    }

    for index_name, mapping in mappings.items():
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            print(f"Created index: {index_name}")
        else:
            print(f"Index already exists: {index_name}")


# ---------------------------------------------------------------------------
# PostgreSQL fetch helpers
# ---------------------------------------------------------------------------

def fetch_records(pg_conn, query: str) -> list[dict[str, Any]]:
    with pg_conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


def fetch_ohlcv(pg_conn) -> list[dict[str, Any]]:
    return fetch_records(
        pg_conn,
        """
        SELECT
            ticker,
            window_start,
            window_end,
            open,
            high,
            low,
            close,
            volume
        FROM gold_ohlcv_candles
        ORDER BY window_start DESC
        LIMIT 1000;
        """,
    )


def fetch_vwap(pg_conn) -> list[dict[str, Any]]:
    return fetch_records(
        pg_conn,
        """
        SELECT
            ticker,
            window_start,
            vwap,
            total_volume
        FROM gold_vwap
        ORDER BY window_start DESC
        LIMIT 500;
        """,
    )


def fetch_volatility(pg_conn) -> list[dict[str, Any]]:
    return fetch_records(
        pg_conn,
        """
        SELECT
            ticker,
            avg_volatility,
            max_volatility,
            green_candle_count AS green_candles,
            red_candle_count AS red_candles
        FROM gold_volatility_analysis;
        """,
    )


# ---------------------------------------------------------------------------
# Elasticsearch indexing
# ---------------------------------------------------------------------------

def to_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: to_json_safe(value) for key, value in record.items()}


def build_doc_id(record: dict[str, Any], fallback_timestamp: str) -> str:
    ticker = record.get("ticker", "unknown")
    window_start = record.get("window_start") or fallback_timestamp
    return f"{ticker}_{window_start}"


def index_records(es: Elasticsearch, index_name: str, records: Iterable[dict[str, Any]]) -> int:
    indexed_at = datetime.now(timezone.utc).isoformat()
    actions = []

    for record in records:
        cleaned = clean_record(record)
        cleaned["indexed_at"] = indexed_at
        actions.append(
            {
                "_op_type": "index",
                "_index": index_name,
                "_id": build_doc_id(cleaned, indexed_at),
                "_source": cleaned,
            }
        )

    if not actions:
        print(f"No records found for '{index_name}'")
        return 0

    success, _ = helpers.bulk(es, actions, raise_on_error=False)
    print(f"Indexed {success} records into '{index_name}'")
    return success


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_once() -> None:
    print("Starting Elasticsearch indexing...")
    es = get_es_client()

    if not es.ping():
        raise ConnectionError(
            f"Could not connect to Elasticsearch at {ELASTICSEARCH_HOST}:{ELASTICSEARCH_PORT}"
        )

    create_indices(es)

    with get_pg_connection() as pg_conn:
        index_records(es, OHLCV_INDEX, fetch_ohlcv(pg_conn))
        index_records(es, VWAP_INDEX, fetch_vwap(pg_conn))
        index_records(es, VOLATILITY_INDEX, fetch_volatility(pg_conn))

    print("Indexing complete.")


def run() -> None:
    if INDEX_INTERVAL_SECONDS <= 0:
        run_once()
        return

    while True:
        try:
            run_once()
        except Exception as exc:
            print(f"Indexing failed: {exc}")
        time.sleep(INDEX_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
