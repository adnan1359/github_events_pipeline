SELECT
    event_id,
    event_type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_login,
    is_public,
    event_timestamp,
    event_date,
    event_hour,
    ingested_at
FROM {{ source('silver', 'events') }}
