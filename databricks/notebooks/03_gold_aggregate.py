
# Gold — business-ready star schema + aggregates

# Silver → Gold. Builds dimension tables (`dim_repos`, `dim_users`), a fact table
# (`fact_events`), and pre-aggregated tables that Power BI reads directly.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("silver_db", "gh_silver")
dbutils.widgets.text("gold_db", "gh_gold")

CATALOG = dbutils.widgets.get("catalog")
SILVER_DB = f"{CATALOG}.{dbutils.widgets.get('silver_db')}"
GOLD_DB = f"{CATALOG}.{dbutils.widgets.get('gold_db')}"

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")
silver = spark.table(f"{SILVER_DB}.events")

def write_gold(df, name, partition_by=None):
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.saveAsTable(f"{GOLD_DB}.{name}")
    print(f"wrote {GOLD_DB}.{name}")



dim_repos = (
    silver.where(F.col("repo_id").isNotNull())
    .groupBy("repo_id", "repo_name")
    .agg(
        F.min("event_timestamp").alias("first_seen"),
        F.max("event_timestamp").alias("last_seen"),
        F.countDistinct("actor_id").alias("contributors"),
    )
)
write_gold(dim_repos, "dim_repos")



dim_users = (
    silver.where(F.col("actor_id").isNotNull())
    .groupBy("actor_id", "actor_login")
    .agg(
        F.min("event_timestamp").alias("first_seen"),
        F.max("event_timestamp").alias("last_seen"),
        F.countDistinct("repo_id").alias("repos_touched"),
    )
)
write_gold(dim_users, "dim_users")


fact_events = silver.select(
    "event_id", "event_type", "actor_id", "repo_id",
    "is_public", "event_timestamp", "event_date", "event_hour",
)
write_gold(fact_events, "fact_events", partition_by="event_date")



agg_daily = (
    silver.groupBy("event_date", "repo_id", "repo_name", "event_type")
    .agg(
        F.count("event_id").alias("event_count"),
        F.countDistinct("actor_id").alias("unique_actors"),
    )
)
write_gold(agg_daily, "agg_daily_activity", partition_by="event_date")




top_repos = (
    silver.where(F.col("event_type") == "WatchEvent")
    .groupBy("repo_id", "repo_name")
    .agg(F.count("event_id").alias("total_stars"))
    .orderBy(F.desc("total_stars"))
)
write_gold(top_repos, "top_repos_by_stars")


print("Gold layer refreshed.")
display(spark.sql(f"SELECT * FROM {GOLD_DB}.top_repos_by_stars LIMIT 20"))
