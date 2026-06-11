import logging
from pyspark.sql import SparkSession
from config.config import * 

# --------------------------------------------------
# Logging Setup 
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("analytics")


# --------------------------------------------------
# Spark Builder
# --------------------------------------------------
def spark_builder():
    logger.info("Initializing Spark Session...")

    spark = (
        SparkSession.builder
        .appName("StockAnalytics")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark Session created successfully")
    return spark


# --------------------------------------------------
# Load Gold Layer
# --------------------------------------------------
def load_data(spark):
    logger.info("Loading Gold OHLCV data...")

    df = spark.read.parquet(f"{GOLD_PATH}/ohlcv")
    df.createOrReplaceTempView("ohlcv")

    logger.info(f"Loaded {df.count()} rows into 'ohlcv' view")

    return df


# --------------------------------------------------
# 1. Latest Candle per Ticker
# --------------------------------------------------
def get_latest_candles(spark):
    logger.info("Computing latest candles per ticker...")

    return spark.sql("""
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY ticker
                       ORDER BY window_start DESC
                   ) AS rn
            FROM ohlcv
        )
        WHERE rn = 1
        ORDER BY ticker
    """)


# --------------------------------------------------
# 2. Price Performance
# --------------------------------------------------
def get_price_performance(spark):
    logger.info("Computing price performance...")

    return spark.sql("""
        SELECT
            ticker,
            ROUND(first_open, 2) AS day_open,
            ROUND(last_close, 2) AS day_close,
            ROUND((last_close - first_open) / first_open * 100, 2) AS pct_change,
            SUM(volume) AS total_volume,
            MAX(high) AS day_high,
            MIN(low) AS day_low
        FROM (
            SELECT
                ticker,
                FIRST_VALUE(open) OVER (
                    PARTITION BY ticker ORDER BY window_start
                ) AS first_open,

                LAST_VALUE(close) OVER (
                    PARTITION BY ticker ORDER BY window_start
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_close,

                volume, high, low
            FROM ohlcv
        )
        GROUP BY ticker, first_open, last_close
        ORDER BY pct_change DESC
    """)


# --------------------------------------------------
# 3. Volume Leaders
# --------------------------------------------------
def get_volume_leaders(spark):
    logger.info("Computing volume leaders...")

    return spark.sql("""
        SELECT
            ticker,
            SUM(volume) AS total_volume,
            COUNT(*) AS candle_count
        FROM ohlcv
        GROUP BY ticker
        ORDER BY total_volume DESC
    """)


# --------------------------------------------------
# 4. Volatility Analysis
# --------------------------------------------------
def get_volatility(spark):
    logger.info("Computing volatility metrics...")

    return spark.sql("""
        SELECT
            ticker,
            ROUND(AVG(price_range_pct), 4) AS avg_volatility,
            ROUND(MAX(price_range_pct), 4) AS max_volatility,
            SUM(CASE WHEN candle_direction = 'green' THEN 1 ELSE 0 END) AS green_candles,
            SUM(CASE WHEN candle_direction = 'red' THEN 1 ELSE 0 END) AS red_candles
        FROM ohlcv
        GROUP BY ticker
        ORDER BY avg_volatility DESC
    """)


# --------------------------------------------------
# Run Pipeline
# --------------------------------------------------
def run():
    spark = spark_builder()

    load_data(spark)

    logger.info("========================================")
    logger.info("STARTING ANALYTICS PIPELINE")
    logger.info("========================================")

    latest = get_latest_candles(spark)
    logger.info("Latest candles computed")
    latest.show(truncate=False)

    perf = get_price_performance(spark)
    logger.info("Price performance computed")
    perf.show(truncate=False)

    volume = get_volume_leaders(spark)
    logger.info("Volume leaders computed")
    volume.show(truncate=False)

    vol = get_volatility(spark)
    logger.info("Volatility computed")
    vol.show(truncate=False)

    logger.info("========================================")
    logger.info("ANALYTICS PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("========================================")


# --------------------------------------------------
if __name__ == "__main__":
    run()