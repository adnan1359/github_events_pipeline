# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — parse, clean, dedupe
# MAGIC
# MAGIC Bronze → Silver. We parse the raw JSON with an explicit schema, flatten the
# MAGIC nested `actor`/`repo` structs, type-cast timestamps, drop malformed rows, and
# MAGIC **MERGE** on `event_id` so re-runs are idempotent (exactly-once at the table).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_db", "gh_bronze")
dbutils.widgets.text("silver_db", "gh_silver")
dbutils.widgets.text("lookback_days", "1")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_DB = f"{CATALOG}.{dbutils.widgets.get('bronze_db')}"
SILVER_DB = f"{CATALOG}.{dbutils.widgets.get('silver_db')}"
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType,
)
from delta.tables import DeltaTable

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------

# Explicit schema = no surprises, no expensive schema inference on every run.
actor_schema = StructType([
    StructField("id", LongType()),
    StructField("login", StringType()),
    StructField("display_login", StringType()),
    StructField("url", StringType()),
    StructField("avatar_url", StringType()),
])
repo_schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("url", StringType()),
])
org_schema = StructType([
    StructField("id", LongType()),
    StructField("login", StringType()),
])
event_schema = StructType([
    StructField("id", StringType()),
    StructField("type", StringType()),
    StructField("actor", actor_schema),
    StructField("repo", repo_schema),
    StructField("org", org_schema),
    StructField("public", BooleanType()),
    StructField("created_at", StringType()),
])

# COMMAND ----------

# Incremental read: only the recent partitions from Bronze.
bronze = (
    spark.table(f"{BRONZE_DB}.raw_events")
    .where(F.col("ingestion_date") >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .where(F.col("raw_json").isNotNull())
)

parsed = bronze.select(
    F.from_json("raw_json", event_schema).alias("e"),
    "ingested_at",
    "kafka_offset",
)

silver = (
    parsed.select(
        F.col("e.id").alias("event_id"),
        F.col("e.type").alias("event_type"),
        F.col("e.actor.id").alias("actor_id"),
        F.col("e.actor.login").alias("actor_login"),
        F.col("e.repo.id").alias("repo_id"),
        F.col("e.repo.name").alias("repo_name"),
        F.col("e.org.login").alias("org_login"),
        F.col("e.public").alias("is_public"),
        F.to_timestamp("e.created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_timestamp"),
        F.col("ingested_at"),
    )
    .where(F.col("event_id").isNotNull() & F.col("event_type").isNotNull())
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .dropDuplicates(["event_id"])           # dedupe within this batch
)

# COMMAND ----------

target = f"{SILVER_DB}.events"

if spark.catalog.tableExists(target):
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(silver.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()           # insert only new events => exactly-once
        .execute()
    )
else:
    (
        silver.write.format("delta")
        .partitionBy("event_date", "event_type")
        .saveAsTable(target)
    )

# COMMAND ----------

# Keep the table healthy (compaction). OPTIMIZE works on Databricks Delta.
spark.sql(f"OPTIMIZE {target}")

display(spark.sql(f"SELECT event_type, COUNT(*) AS n FROM {target} GROUP BY event_type ORDER BY n DESC"))
