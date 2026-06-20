from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import logging
import sys
from pathlib import Path
from delta import configure_spark_with_delta_pip

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.config import (
    BRONZE_PATH,
    SILVER_PATH,
    TRIGGER_INTERVAL,
    BRONZE_SCHEMA  
)

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("silver")


# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────
def spark_builder():
    builder = (
        SparkSession.builder
        .appName("SilverStream")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        .config("spark.sql.streaming.fileSource.log.cleanupDelay", "0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ─────────────────────────────────────────────
# Read Bronze Stream
# ─────────────────────────────────────────────
def read_bronze_stream(spark: SparkSession) -> DataFrame:

    logger.info(f"Reading bronze stream: {BRONZE_PATH}/ticks")

    return (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 5)
    .load(f"{BRONZE_PATH}/ticks")
)


# ─────────────────────────────────────────────
# Clean + Enrich (STREAM SAFE VERSION)
# ─────────────────────────────────────────────
def clean_and_enrich(df: DataFrame) -> DataFrame:

    # ── Basic data quality filters ─────────────────────────────
    cleaned = (
        df
        .filter(col("event_id").isNotNull())
        .filter(col("ticker").isNotNull())
        .filter(col("event_ts").isNotNull())
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("bid").isNotNull() & (col("bid") > 0))
        .filter(col("ask").isNotNull() & (col("ask") > 0))
        .filter(col("ask") >= col("bid"))
        .filter(col("volume").isNotNull() & (col("volume") > 0))
    )

    # ── IMPORTANT: Deduplication (fundamental fix) ─────────────
    deduped = cleaned.dropDuplicates(["event_id"])

    # ── Watermark (event-time correctness) ─────────────────────
    # allows late data handling + safe future joins/aggregations
    timed = deduped.withWatermark("event_ts", "10 minutes")

    # ── Enrichment ──────────────────────────────────────────────
    enriched = (
        timed
        .withColumn("spread", round(col("ask") - col("bid"), 4))
        .withColumn("spread_pct", round(((col("ask") - col("bid")) / col("price")) * 100, 4))
        .withColumn("trade_date", to_date(col("event_ts")))
        .withColumn("trade_hour", hour(col("event_ts")))
        .withColumn("trade_minute", minute(col("event_ts")))
        .withColumn("notional_value", round(col("price") * col("volume"), 2))
        .withColumn(
            "price_bucket",
            when(col("price") < 50, "under_50")
            .when(col("price") < 100, "50_to_100")
            .when(col("price") < 500, "100_to_500")
            .otherwise("over_500")
        )
    )

    return enriched


# ─────────────────────────────────────────────
# Write Silver (Parquet only)
# ─────────────────────────────────────────────
def write_silver(df: DataFrame) -> None:

   (
    df.write
    .format("delta")
    .mode("append")
    .partitionBy("trade_date", "ticker")
    .save(f"{SILVER_PATH}/ticks")
)


# ─────────────────────────────────────────────
# Batch Processor
# ─────────────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int) -> None:

    try:
        logger.info(f"[Batch {batch_id}] Started")

        enriched_df = clean_and_enrich(batch_df)

        count = enriched_df.count()

        if count == 0:
            logger.info(f"[Batch {batch_id}] Empty after cleaning")
            return

        logger.info(f"[Batch {batch_id}] Processing {count:,} rows")

        # Projection layer (kept clean for downstream Gold)
        output_df = enriched_df.select(
            "event_id",
            "ticker",
            "price",
            "volume",
            "bid",
            "ask",
            "spread",
            "spread_pct",
            "notional_value",
            "price_bucket",
            "event_ts",
            "trade_date",
            "trade_hour",
            "trade_minute"
        )

        write_silver(output_df)

        logger.info(f"[Batch {batch_id}] Successfully written")

    except Exception as e:
        logger.error(f"[Batch {batch_id}] Failed: {str(e)}", exc_info=True)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():

    spark = spark_builder()

    logger.info("=" * 50)
    logger.info("Silver Stream Starting")
    logger.info(f"Read  : {BRONZE_PATH}/ticks")
    logger.info(f"Write : {SILVER_PATH}/ticks")
    logger.info("=" * 50)

    raw_stream = read_bronze_stream(spark)

    query = (
        raw_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{SILVER_PATH}/_checkpoint")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("silver_ticks")
        .start()
    )

    logger.info("Silver stream running")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Silver stream...")
        query.stop()
        logger.info("Stopped successfully")


if __name__ == "__main__":
    main()