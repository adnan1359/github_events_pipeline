{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    partition_by=['event_date']
) }}

SELECT
    se.event_id,
    se.event_type,
    se.actor_id,
    se.repo_id,
    se.is_public,
    se.event_timestamp,
    se.event_date,
    se.event_hour,
    se.ingested_at
FROM {{ ref('silver_events') }} AS se

{% if is_incremental() %}
    -- only pull rows newer than what we already loaded
    WHERE
        se.ingested_at
        > (
            SELECT COALESCE(MAX(t.ingested_at), TIMESTAMP '1970-01-01 00:00:00')
            FROM {{ this }} AS t
        )
{% endif %}
