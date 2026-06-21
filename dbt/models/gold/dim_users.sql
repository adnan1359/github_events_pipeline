SELECT
    actor_id,
    actor_login,
    MIN(event_timestamp)        AS first_seen,
    MAX(event_timestamp)        AS last_seen,
    COUNT(DISTINCT repo_id)     AS repos_touched
FROM {{ ref('silver_events') }}
WHERE actor_id IS NOT NULL
GROUP BY actor_id, actor_login
