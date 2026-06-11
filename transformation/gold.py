import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
from config.config import *


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("gold")




# ─────────────────────────────────────────────
# Spark
# ─────────────────────────────────────────────

def spark_builder():
    spark = (
        SparkSession.builder
        .appName("gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────
# Read Silver Stream
# ─────────────────────────────────────────────

def read_silver_stream(spark):
    return (
        spark.readStream
        .format("parquet")
        .schema(SILVER_SCHEMA)
        .option("maxFilesPerTrigger", 5)
        .load(f"{SILVER_PATH}/ticks")
    )


# ─────────────────────────────────────────────
# OHLCV builder
# ─────────────────────────────────────────────

def build_ohlcv(df: DataFrame) -> DataFrame:
    return (
        df
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            window("event_ts", "1 minute"),
            col("ticker")
        )
        .agg(
            min_by("price", "event_ts").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            max_by("price", "event_ts").alias("close"),
            sum("volume").alias("volume"),
            count("*").alias("tick_count"),
            round(avg("price"), 4).alias("vwap"),
            round(
                avg((col("ask") - col("bid")) / col("price") * 100),
                4
            ).alias("avg_spread_pct"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "ticker",
            "open", "high", "low", "close", "volume",
            "tick_count", "vwap", "avg_spread_pct"
        )
        .withColumn("price_range", round(col("high") - col("low"), 2))
        .withColumn(
            "price_range_pct",
            (col("high") - col("low")) / col("open") * 100
        )
        .withColumn(
            "candle_direction",
            when(col("close") >= col("open"), "green").otherwise("red")
        )
    )


# ─────────────────────────────────────────────
# Top movers
# ─────────────────────────────────────────────

def build_top_movers(df: DataFrame):
    return (
        df
        .withColumn(
            "pct_change",
            (col("close") - col("open")) / col("open") * 100
        )
        .withColumn("abs_pct_change", abs(col("pct_change")))
        .orderBy(col("abs_pct_change").desc())
    )


# ─────────────────────────────────────────────
# FOREACH BATCH (WITH LOGGING)
# ─────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int):

    try:
        logger.info(f"[Batch {batch_id}] ===============================")
        logger.info(f"[Batch {batch_id}] GOLD BATCH STARTED")

        input_count = batch_df.count()
        logger.info(f"[Batch {batch_id}] Input silver rows: {input_count:,}")

        if input_count == 0:
            logger.info(f"[Batch {batch_id}] Empty batch — skipping")
            return

        ohlcv = build_ohlcv(batch_df).cache()

        ohlcv_count = ohlcv.count()
        logger.info(f"[Batch {batch_id}] OHLCV rows generated: {ohlcv_count:,}")

        if ohlcv_count == 0:
            logger.info(f"[Batch {batch_id}] No aggregates produced")
            ohlcv.unpersist()
            return

        movers = build_top_movers(ohlcv)
        top_movers_sample = movers.limit(5).collect()

        logger.info(f"[Batch {batch_id}] Top movers sample:")
        for row in top_movers_sample:
            logger.info(
                f"[Batch {batch_id}] "
                f"{row['ticker']} | "
                f"change={row['pct_change']:.2f}%"
            )

        logger.info(f"[Batch {batch_id}] Writing OHLCV to gold layer...")

        (
            ohlcv.write
            .mode("append")
            .partitionBy("window_start")
            .parquet(f"{GOLD_PATH}/ohlcv")
        )

        logger.info(f"[Batch {batch_id}] Write successful")

        ohlcv.unpersist()

        logger.info(f"[Batch {batch_id}] GOLD BATCH COMPLETED")
        logger.info(f"[Batch {batch_id}] ===============================")

    except Exception as e:
        logger.error(
            f"[Batch {batch_id}] GOLD PIPELINE FAILED: {str(e)}",
            exc_info=True
        )


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():

    spark = spark_builder()

    logger.info("=" * 60)
    logger.info("GOLD STREAM STARTING")
    logger.info(f"Source: {SILVER_PATH}/ticks")
    logger.info(f"Output: {GOLD_PATH}/ohlcv")
    logger.info("=" * 60)

    raw = read_silver_stream(spark)

    query = (
        raw.writeStream
        .foreachBatch(process_batch)
        .queryName("gold_ohlcv")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/gold_ohlcv")
        .start()
    )

    logger.info("Gold stream running...")

    query.awaitTermination()


if __name__ == "__main__":
    main()