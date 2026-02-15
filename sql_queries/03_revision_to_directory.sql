-- Step 3: Revision → Root Directory

CREATE TABLE default.revision_directory AS
SELECT
    id AS revision_id,
    directory AS directory_id
FROM swh_graph_2025_10_08.revision;

CREATE TABLE default.url_revision_directory AS
SELECT
    u.url,
    u.visit_date,
    rd.directory_id
FROM default.url_date_branch u
JOIN default.revision_directory rd
    ON u.revision_id = rd.revision_id;
