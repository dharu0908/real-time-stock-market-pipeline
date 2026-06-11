import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ─────────────────────────────────────────────
# PATH SETUP
# ─────────────────────────────────────────────
sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC,
    BRONZE_PATH,
    CHECKPOINT_PATH,
    TRIGGER_INTERVAL,
    MAX_OFFSETS_PER_TRIGGER,
    TICK_SCHEMA,   
)

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("bronze_stream")


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
def spark_builder() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BronzeKafkaStream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.network.timeout", "120s")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ─────────────────────────────────────────────
# READ FROM KAFKA
# ─────────────────────────────────────────────
def read_from_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
        .select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )


# ─────────────────────────────────────────────
# PARSE + VALIDATE
# ─────────────────────────────────────────────
def parse_and_validate(raw: DataFrame) -> tuple[DataFrame, DataFrame]:

    parsed = raw.withColumn(
        "data",
        F.from_json(F.col("raw_json"), TICK_SCHEMA)
    )

    # ---------------- GOOD RECORDS ----------------
    good = (
        parsed
        .filter(
            F.col("data.event_id").isNotNull()
            & F.col("data.ticker").isNotNull()
            & F.col("data.price").isNotNull()
        )
        .select(
            # raw + structured (IMPORTANT for bronze)
            "raw_json",

            F.col("data.event_id").alias("event_id"),
            F.col("data.ticker").alias("ticker"),
            F.col("data.price").alias("price"),
            F.col("data.bid").alias("bid"),
            F.col("data.ask").alias("ask"),
            F.col("data.volume").alias("volume"),

            # safer timestamp parsing
            F.to_timestamp("data.event_timestamp").alias("event_ts"),

            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("kafka_timestamp"),

            F.current_timestamp().alias("ingested_at"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )

    # ---------------- BAD RECORDS (DLQ) ----------------
    bad = (
        parsed
        .filter(
            F.col("data.event_id").isNull()
            | F.col("data.ticker").isNull()
            | F.col("data.price").isNull()
        )
        .select(
            "raw_json",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.current_timestamp().alias("ingested_at"),
        )
        .dropDuplicates(["kafka_partition", "kafka_offset"])
    )

    return good, bad


# ─────────────────────────────────────────────
# FOREACH BATCH
# ─────────────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int) -> None:

    good, bad = parse_and_validate(batch_df)

    

    
    good_exists = good.limit(1).count() > 0
    bad_exists = bad.limit(1).count() > 0

    if not good_exists and not bad_exists:
        log.info(f"[Batch {batch_id}] Empty batch")
        return

    log.info(f"[Batch {batch_id}] Processing batch")
    

    # ───────────── BRONZE WRITE ─────────────
    if good_exists:
        (
            good.write
            .mode("append")
            .partitionBy("event_date")   # cleaner bronze design
            .parquet(f"{BRONZE_PATH}/ticks")
        )

    # ───────────── DLQ WRITE ─────────────
    if bad_exists:
        (
            bad.write
            .mode("append")
            .parquet(f"{BRONZE_PATH}/dead_letter")
        )


# ─────────────────────────────────────────────
# MAIN STREAM
# ─────────────────────────────────────────────
def main():
    spark = spark_builder()

    log.info("=" * 60)
    log.info("----BRONZE STREAM STARTING----")
    log.info(f"Topic     : {TOPIC}")
    log.info(f"Output    : {BRONZE_PATH}")
    log.info(f"Checkpoint: {CHECKPOINT_PATH}/bronze")
    log.info(f"Trigger   : {TRIGGER_INTERVAL}")
    log.info("=" * 60)

    raw_stream = read_from_kafka(spark)

    query = (
        raw_stream
        .writeStream
        .queryName("bronze_ticks")
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    log.info("Bronze stream running...")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping stream...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
