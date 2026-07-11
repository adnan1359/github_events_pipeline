{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['event_date']
) }}

SELECT
    se.event_date,
    se.repo_id,
    se.repo_name,
    se.event_type,
    COUNT(se.event_id) AS event_count,
    COUNT(DISTINCT se.actor_id) AS unique_actors,
    MAX(se.ingested_at) AS ingested_at
FROM {{ ref('silver_events') }} AS se

{% if is_incremental() %}
    -- Re-aggregate only the dates affected by newly ingested data
    WHERE se.event_date IN (
        SELECT DISTINCT sub.event_date
        FROM {{ ref('silver_events') }} AS sub
        WHERE
            sub.ingested_at
            > (
                SELECT
                    COALESCE(
                        MAX(t.ingested_at), TIMESTAMP '1970-01-01 00:00:00'
                    )
                FROM {{ this }} AS t
            )
    )
{% endif %}

GROUP BY se.event_date, se.repo_id, se.repo_name, se.event_type
