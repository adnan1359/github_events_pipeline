{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['event_date']
) }}

SELECT
    event_date,
    repo_id,
    repo_name,
    event_type,
    COUNT(event_id)             AS event_count,
    COUNT(DISTINCT actor_id)    AS unique_actors,
    MAX(ingested_at)            AS ingested_at
FROM {{ ref('silver_events') }}

{% if is_incremental() %}
  -- Re-aggregate only the dates affected by newly ingested data
  WHERE event_date IN (
      SELECT DISTINCT event_date
      FROM {{ ref('silver_events') }}
      WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP'1970-01-01 00:00:00') FROM {{ this }})
  )
{% endif %}

GROUP BY event_date, repo_id, repo_name, event_type
