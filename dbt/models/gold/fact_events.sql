{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    partition_by=['event_date']
) }}

SELECT
    event_id,
    event_type,
    actor_id,
    repo_id,
    is_public,
    event_timestamp,
    event_date,
    event_hour,
    ingested_at
FROM {{ ref('silver_events') }}

{% if is_incremental() %}
  -- only pull rows newer than what we already loaded
  WHERE ingested_at > (SELECT COALESCE(MAX(ingested_at), TIMESTAMP'1970-01-01 00:00:00') FROM {{ this }})
{% endif %}
