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
    event_hour
FROM {{ ref('silver_events') }}

{% if is_incremental() %}
  -- only pull rows newer than what we already loaded
  WHERE event_date >= (SELECT COALESCE(MAX(event_date), DATE'1970-01-01') FROM {{ this }})
{% endif %}
