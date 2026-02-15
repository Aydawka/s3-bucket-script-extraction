-- Step 5: Filter GitHub Repositories Only

CREATE TABLE default.filtered_github_total_table AS
SELECT *
FROM default.url_readme_content
WHERE url LIKE 'https://github.com/%';
