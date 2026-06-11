from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType,TimestampType,LongType

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
TOPIC="stock_ticks"

TOPIC_PARTITIONS = 3
TOPIC_REPLICATION_FACTOR = 1
TRIGGER_INTERVAL = "10 seconds"
MAX_OFFSETS_PER_TRIGGER = 5000

DEFAULT_RPS = 10

TICK_SCHEMA = StructType([ 
    StructField("event_id", StringType(), True),
    StructField("ticker", StringType(), True), 
    StructField("price", DoubleType(), True), 
    StructField("bid", DoubleType(), True), 
    StructField("ask", DoubleType(), True), 
    StructField("volume", IntegerType(), True), 
    StructField("event_timestamp", StringType(), True),
    ])

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
])


SILVER_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("event_id", StringType()),
    StructField("price", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume", IntegerType()),   # KEEP INTEGER (as requested)
    StructField("event_ts", TimestampType()),
    StructField("ingested_at", TimestampType()),
])

PRODUCER_CONFIG = {
    "acks": "all",
    "compression.type": "lz4",
    "linger.ms": 5,
    "retries": 3,
}

TICKERS = [
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "GOOGL", # Alphabet
    "AMZN",  # Amazon
    "NVDA",  # NVIDIA
    "META",  # Meta
    "TSLA"   # Tesla
]

PRICES={
  "AAPL": 312.51,
  "MSFT": 468.20,
  "GOOGL": 196.45,
  "AMZN": 214.33,
  "NVDA": 141.72,
  "META": 529.18,
  "TSLA": 178.94
}

BASE_PATH        = "./data"
RAW_PATH         = f"{BASE_PATH}/raw"          # original CSV lives here
CHECKPOINT_PATH  = f"{BASE_PATH}/checkpoints"  # Spark Streaming state
BRONZE_PATH      = f"{BASE_PATH}/bronze"       # raw parsed events
SILVER_PATH      = f"{BASE_PATH}/silver"       # cleaned + enriched
GOLD_PATH        = f"{BASE_PATH}/gold"         # aggregated analytics
SERVING_PATH     = f"{BASE_PATH}/serving"  
