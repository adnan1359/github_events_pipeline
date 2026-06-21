-- Thin staging view over the Silver Delta table so Gold models depend on ref(),
-- not a hard-coded table name. Keeps lineage clean in dbt docs.
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
    event_hour
FROM {{ source('silver', 'events') }}
