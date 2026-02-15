-- Step 7: Partition by Last Hex of SHA1

CREATE TABLE part1 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('0','1','2');

CREATE TABLE part2 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('3','4','5');

CREATE TABLE part3 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('6','7','8');

CREATE TABLE part4 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('9','a','b');

CREATE TABLE part5 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('c','d');

CREATE TABLE part6 AS
SELECT * FROM default.filtered_github_unique
WHERE SUBSTR(content_sha1_git, -1) IN ('e','f');
