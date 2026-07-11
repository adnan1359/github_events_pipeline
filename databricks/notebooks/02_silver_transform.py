# Bronze → Silver. We parse the raw JSON with an explicit schema, flatten the
# nested `actor`/`repo` structs, type-cast timestamps, drop malformed rows, and
# **MERGE** on `event_id` so re-runs are idempotent

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("bronze_db", "gh_bronze")
dbutils.widgets.text("silver_db", "gh_silver")
dbutils.widgets.text("lookback_days", "1")
dbutils.widgets.text("run_date", "")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_DB = f"{CATALOG}.{dbutils.widgets.get('bronze_db')}"
SILVER_DB = f"{CATALOG}.{dbutils.widgets.get('silver_db')}"
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
RUN_DATE = dbutils.widgets.get("run_date")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------

actor_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("login", StringType()),
        StructField("display_login", StringType()),
        StructField("url", StringType()),
        StructField("avatar_url", StringType()),
    ]
)
repo_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("url", StringType()),
    ]
)
org_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("login", StringType()),
    ]
)
event_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField("actor", actor_schema),
        StructField("repo", repo_schema),
        StructField("org", org_schema),
        StructField("public", BooleanType()),
        StructField("created_at", StringType()),
    ]
)


base_date = F.to_date(F.lit(RUN_DATE)) if RUN_DATE else F.current_date()
bronze = (
    spark.table(f"{BRONZE_DB}.raw_events")
    .where(F.col("ingestion_date") >= F.date_sub(base_date, LOOKBACK_DAYS))
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
    .dropDuplicates(["event_id"])
)

# COMMAND ----------

target = f"{SILVER_DB}.events"

if spark.catalog.tableExists(target):
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(silver.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (silver.write.format("delta").partitionBy("event_date", "event_type").saveAsTable(target))

# COMMAND ----------

# Apply Delta Table Constraints (checks value integrity)
try:
    spark.sql(
        f"ALTER TABLE {target} ADD CONSTRAINT check_event_timestamp CHECK (event_timestamp > '2020-01-01T00:00:00Z')"
    )
except Exception as e:
    if "already exists" not in str(e):
        print(f"Warning: could not add timestamp constraint: {e}")

# COMMAND ----------

spark.sql(f"OPTIMIZE {target}")

display(
    spark.sql(f"SELECT event_type, COUNT(*) AS n FROM {target} GROUP BY event_type ORDER BY n DESC")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Validation Checks

# COMMAND ----------

# 1. Verify duplicates count is 0
dupes_df = spark.table(target).groupBy("event_id").count().where("count > 1")
dupes_count = dupes_df.count()
if dupes_count > 0:
    raise AssertionError(
        f"DATA QUALITY FAILURE: Found {dupes_count} duplicate event_ids in Silver table {target}!"
    )

# 2. Verify critical columns have 0 null values
null_checks = ["actor_id", "repo_id", "event_timestamp"]
for col_name in null_checks:
    null_count = spark.table(target).where(F.col(col_name).isNull()).count()
    if null_count > 0:
        raise AssertionError(
            f"DATA QUALITY FAILURE: Found {null_count} null values in critical column '{col_name}' of Silver table {target}!"
        )

print("✅ Silver data quality validation checks passed successfully!")
