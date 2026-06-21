{{ config(
    materialized='incremental',
    unique_key=['event_date', 'repo_id', 'event_type'],
    incremental_strategy='merge',
    partition_by=['event_date']
) }}

SELECT
    event_date,
    repo_id,
    repo_name,
    event_type,
    COUNT(event_id)             AS event_count,
    COUNT(DISTINCT actor_id)    AS unique_actors
FROM {{ ref('silver_events') }}

{% if is_incremental() %}
  WHERE event_date >= (SELECT COALESCE(MAX(event_date), DATE'1970-01-01') FROM {{ this }})
{% endif %}

GROUP BY event_date, repo_id, repo_name, event_type
