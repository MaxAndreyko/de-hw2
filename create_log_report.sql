CREATE TABLE IF NOT EXISTS de.maka_log (
  dt DATE,
  link VARCHAR(50),
  user_agent VARCHAR(200),
  region VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS de.maka_log_report (
  region VARCHAR(30),
  browser VARCHAR(10)
);
WITH temp_ip AS (
    SELECT
        SPLIT_PART(de.ip.data, E'\t', 1) AS ip,
        SPLIT_PART(de.ip.data, E'\t', 2) AS region
    FROM de.ip
),
temp_log AS (
    SELECT
        SPLIT_PART(de.log.data, E'\t', 1) AS ip,
        TO_TIMESTAMP(SPLIT_PART(de.log.data, E'\t', 4), 'YYYYMMDDHH24MISS')::DATE AS dt,
        SPLIT_PART(de.log.data, E'\t', 5) AS link,
        SPLIT_PART(de.log.data, E'\t', 8) AS user_agent
    FROM de.log
),
insert_log AS (
INSERT INTO de.maka_log (dt, link, user_agent, region)
SELECT
    temp_log.dt,
    temp_log.link,
    temp_log.user_agent,
    temp_ip.region
FROM temp_log
JOIN temp_ip ON temp_log.ip = temp_ip.ip
WHERE temp_ip.region IS NOT NULL
),
struct_log AS (
    SELECT
        temp_log.dt,
        temp_log.link,
        temp_log.user_agent,
        SPLIT_PART(SPLIT_PART(temp_log.user_agent, '', 1), '/', 1) AS browser,
        temp_ip.region,
        temp_ip.ip
    FROM temp_log
    JOIN temp_ip ON temp_log.ip = temp_ip.ip
    WHERE temp_ip.region IS NOT NULL
),
unique_ip_browser AS (
    SELECT DISTINCT ON (ip, browser)
        ip,
        browser,
        region
    FROM struct_log
),
use_count AS (
    SELECT
      region,
      browser,
      COUNT(*) AS use_count
    FROM unique_ip_browser
    GROUP BY region, browser
),
max_use_count AS (
    SELECT
        region,
        browser,
        use_count,
        RANK() OVER (PARTITION BY region ORDER BY use_count DESC) AS rank
    FROM use_count
)
INSERT INTO de.maka_log_report (region, browser)
SELECT
    region,
    browser
FROM max_use_count
WHERE rank = 1;
