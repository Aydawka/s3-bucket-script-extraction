-- Step 1: Origin → Visit → Snapshot
-- Step 0: Register Software Heritage 2025 dataset (if not already present)

CREATE EXTERNAL TABLE IF NOT EXISTS swh_graph_2025_10_08.origin (
    id STRING,
    url STRING
)
STORED AS PARQUET
LOCATION 's3://softwareheritage/graph/2025-10-08/origin/';


CREATE TABLE default.url_and_date AS
SELECT
    o.url,
    ovs.date AS visit_date,
    ovs.snapshot_id
FROM swh_graph_2025_10_08.origin o
JOIN swh_graph_2025_10_08.origin_visit_status ovs
    ON o.url = ovs.origin;
