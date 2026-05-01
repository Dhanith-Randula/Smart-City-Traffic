

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)


KAFKA_BOOTSTRAP  = "kafka:9092"
TOPIC_RAW        = "traffic-raw"
CHECKPOINT_BASE  = "/opt/spark/checkpoints"

PG_URL  = "jdbc:postgresql://postgres:5432/trafficdb"
PG_OPTS = {
    "driver":   "org.postgresql.Driver",
    "user":     "trafficuser",
    "password": "trafficpass",
}

CRITICAL_SPEED_THRESHOLD = 10.0   
WINDOW_DURATION          = "5 minutes"
WATERMARK_DELAY          = "10 minutes"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK-PROCESSOR] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


RAW_SCHEMA = StructType([
    StructField("sensor_id",     StringType(),    nullable=False),
    StructField("junction_name", StringType(),    nullable=False),
    StructField("timestamp",     StringType(),    nullable=False),  
    StructField("vehicle_count", IntegerType(),   nullable=False),
    StructField("avg_speed",     DoubleType(),    nullable=False),
])



def write_to_postgres(df, table: str, mode: str = "append"):
    """Write a DataFrame to PostgreSQL via JDBC."""
    (df.write
       .format("jdbc")
       .option("url",    PG_URL)
       .option("dbtable", table)
       .option("driver",   PG_OPTS["driver"])
       .option("user",     PG_OPTS["user"])
       .option("password", PG_OPTS["password"])
       .mode(mode)
       .save())



def compute_congestion_index(avg_speed_col, vehicle_count_col):
    """
    Congestion Index (0-100):
      - Speed component  (70%): normalised inverse of speed (max ref = 70 km/h)
      - Volume component (30%): normalised vehicle count   (max ref = 200 vehicles)
    Higher index = more congested.
    """
    speed_component  = F.greatest(F.lit(0.0),
                           (F.lit(1.0) - (avg_speed_col / F.lit(70.0)))) * F.lit(70.0)
    volume_component = F.least(F.lit(1.0),
                           (vehicle_count_col / F.lit(200.0))) * F.lit(30.0)
    return F.round(speed_component + volume_component, 2)


def congestion_level(index_col):
    """Map numeric index to human-readable level."""
    return (
        F.when(index_col >= 75, "CRITICAL")
         .when(index_col >= 50, "HIGH")
         .when(index_col >= 25, "MODERATE")
         .otherwise("LOW")
    )



def process_raw_batch(batch_df, batch_id: int):
    """Sink 1 – persist every raw event to traffic_raw."""
    count = batch_df.count()
    if count == 0:
        return
    log.info("[Batch %d] Writing %d raw events → traffic_raw", batch_id, count)
    out = batch_df.select(
        F.col("sensor_id"),
        F.col("junction_name"),
        F.col("event_time").alias("event_time"),
        F.col("vehicle_count"),
        F.col("avg_speed"),
    )
    write_to_postgres(out, "traffic_raw")


def process_window_batch(batch_df, batch_id: int):
    """Sink 2 – 5-minute window aggregates → congestion_windows."""
    count = batch_df.count()
    if count == 0:
        return
    log.info("[Batch %d] Writing %d window rows → congestion_windows", batch_id, count)
    write_to_postgres(batch_df, "congestion_windows")


def process_alert_batch(batch_df, batch_id: int):
    """Sink 3 – critical-speed alerts → critical_traffic."""
    count = batch_df.count()
    if count == 0:
        return
    log.warning("[Batch %d] ⚠  Writing %d CRITICAL alerts → critical_traffic", batch_id, count)
    write_to_postgres(batch_df, "critical_traffic")



def main():
    log.info("Initialising SparkSession …")

    spark = (SparkSession.builder
             .appName("SmartCityTrafficProcessor")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession ready.")

   
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", TOPIC_RAW)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load())

    
    parsed_df = (kafka_df
                 .select(F.from_json(
                     F.col("value").cast("string"), RAW_SCHEMA
                 ).alias("data"))
                 .select("data.*")
                 .withColumn("event_time",
                             F.to_timestamp(F.col("timestamp")))   
                 .withWatermark("event_time", WATERMARK_DELAY))    

    
    raw_query = (parsed_df
                 .writeStream
                 .foreachBatch(process_raw_batch)
                 .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw")
                 .trigger(processingTime="5 seconds")
                 .start())

    log.info("Stream query 'raw_events' started.")

   
    windowed_df = (parsed_df
                   .groupBy(
                       F.col("sensor_id"),
                       F.col("junction_name"),
                       F.window(F.col("event_time"), WINDOW_DURATION),
                   )
                   .agg(
                       F.avg("vehicle_count").alias("avg_vehicle_count"),
                       F.avg("avg_speed").alias("avg_speed"),
                   )
                   .withColumn(
                       "congestion_index",
                       compute_congestion_index(
                           F.col("avg_speed"), F.col("avg_vehicle_count")
                       )
                   )
                   .withColumn("congestion_level", congestion_level(F.col("congestion_index")))
                   .select(
                       F.col("sensor_id"),
                       F.col("junction_name"),
                       F.col("window.start").alias("window_start"),
                       F.col("window.end").alias("window_end"),
                       F.round("avg_vehicle_count", 2).alias("avg_vehicle_count"),
                       F.round("avg_speed", 2).alias("avg_speed"),
                       F.col("congestion_index"),
                       F.col("congestion_level"),
                   ))

    
    window_query = (windowed_df
                    .writeStream
                    .foreachBatch(process_window_batch)
                    .option("checkpointLocation", f"{CHECKPOINT_BASE}/windows")
                    .outputMode("update")
                    .trigger(processingTime="30 seconds")
                    .start())

    log.info("Stream query 'congestion_windows' started.")

    
    alert_df = (parsed_df
                .filter(F.col("avg_speed") < CRITICAL_SPEED_THRESHOLD)
                .withColumn(
                    "congestion_index",
                    compute_congestion_index(
                        F.col("avg_speed"), F.col("vehicle_count")
                    )
                )
                .withColumn(
                    "alert_message",
                    F.concat(
                        F.lit("CRITICAL CONGESTION at "),
                        F.col("junction_name"),
                        F.lit(": speed="),
                        F.col("avg_speed").cast("string"),
                        F.lit(" km/h, vehicles="),
                        F.col("vehicle_count").cast("string"),
                    )
                )
                .select(
                    F.col("sensor_id"),
                    F.col("junction_name"),
                    F.col("event_time").alias("alert_time"),
                    F.col("vehicle_count"),
                    F.col("avg_speed"),
                    F.col("congestion_index"),
                    F.col("alert_message"),
                ))

    
    alert_query = (alert_df
                   .writeStream
                   .foreachBatch(process_alert_batch)
                   .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts")
                   .trigger(processingTime="5 seconds")   
                   .start())

    log.info("Stream query 'critical_alerts' started.")
    log.info("=" * 60)
    log.info("All streaming queries running. Waiting for termination …")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()