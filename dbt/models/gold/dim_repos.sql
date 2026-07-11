SELECT
    repo_id,
    repo_name,
    MIN(event_timestamp) AS first_seen,
    MAX(event_timestamp) AS last_seen,
    COUNT(DISTINCT actor_id) AS contributors
FROM {{ ref('silver_events') }}
WHERE repo_id IS NOT NULL
GROUP BY repo_id, repo_name
