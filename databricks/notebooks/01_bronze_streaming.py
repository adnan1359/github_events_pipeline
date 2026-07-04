# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Kafka → Delta (Structured Streaming)
# MAGIC
# MAGIC Reads from Aiven Kafka over SSL and lands the **raw** event JSON in a Delta
# MAGIC table, untouched.
# MAGIC
# MAGIC **Free Edition / serverless notes:**
# MAGIC - `Trigger.availableNow` is the only supported trigger on serverless — it drains
# MAGIC   whatever is in Kafka, then stops. Airflow re-runs this hourly to keep it fresh.
# MAGIC - The Kafka connector is **preinstalled** on Databricks — no Maven library needed.
# MAGIC - Certs + checkpoints live in a **Unity Catalog Volume** (`/Volumes/...`), because
# MAGIC   serverless has no `/dbfs/` mount. See `databricks/README.md` for setup.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_db", "gh_bronze")
dbutils.widgets.text("volume_path", "/Volumes/workspace/gh_bronze/pipeline")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_DB = dbutils.widgets.get("bronze_db")
VOLUME = dbutils.widgets.get("volume_path")

TRUSTSTORE = f"{VOLUME}/certs/client.truststore.jks"
KEYSTORE = f"{VOLUME}/certs/client.keystore.jks"
CHECKPOINT = f"{VOLUME}/checkpoints/bronze_raw_events"
TABLE = f"{CATALOG}.{BRONZE_DB}.raw_events"



from pyspark.sql.functions import col, current_timestamp, to_date

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_DB}")

KAFKA_BOOTSTRAP = dbutils.secrets.get("kafka-secrets", "bootstrap-servers")
TRUSTSTORE_PW = dbutils.secrets.get("kafka-secrets", "ssl-truststore-password")
KEYSTORE_PW = dbutils.secrets.get("kafka-secrets", "ssl-keystore-password")


kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("kafka.security.protocol", "SSL")
    .option("kafka.ssl.truststore.location", TRUSTSTORE)  
    .option("kafka.ssl.truststore.password", TRUSTSTORE_PW)
    .option("kafka.ssl.keystore.location", KEYSTORE)
    .option("kafka.ssl.keystore.password", KEYSTORE_PW)
    .option("subscribe", "github.events.raw")
    .option("startingOffsets", "earliest")  
    .option("maxOffsetsPerTrigger", 50000) 
    .option("failOnDataLoss", "false")
    .load()
)

# COMMAND ----------

bronze_df = (
    kafka_df.select(
        col("key").cast("string").alias("event_id"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
    )
    .withColumn("ingestion_date", to_date("ingested_at"))
)

# COMMAND ----------

(
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT) 
    .option("mergeSchema", "true")
    .partitionBy("ingestion_date")
    .trigger(availableNow=True)               
    .toTable(TABLE)
    .awaitTermination()
)

display(spark.sql(f"SELECT COUNT(*) AS rows, MAX(ingested_at) AS latest FROM {TABLE}"))
