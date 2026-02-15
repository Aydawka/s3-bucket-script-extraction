-- Step 4: Directory → README → Content SHA

CREATE TABLE default.directory_entry_readme AS
SELECT
    directory_id,
    target AS content_sha1_git
FROM swh_graph_2025_10_08.directory_entry
WHERE type = 'file'
  AND name IN (
      X'524541444D452E6D64',  -- README.md
      X'726561646D652E6D64',  -- readme.md
      X'524541444D45',        -- README
      X'524541444D452E747874' -- README.txt
  );

CREATE TABLE default.url_readme_content AS
SELECT
    u.url,
    u.visit_date,
    d.content_sha1_git
FROM default.url_revision_directory u
JOIN default.directory_entry_readme d
    ON u.directory_id = d.directory_id;
