-- Core Web Page Performance (by URL & title)
-- Creates: marketing_reports.core_web_page_performance

#standardSQL
CREATE OR REPLACE TABLE `marketing_reports.core_web_page_performance` 
PARTITION BY event_date AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    event_name,
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'page_title') AS page_title,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'engagement_time_msec') AS engagement_time_msec
  FROM `marketing_data.analytics_448974598.events_*`
  WHERE _TABLE_SUFFIX = '{{ ds_nodash }}'
)
SELECT
  event_date,
  page_location,
  page_title,
  COUNTIF(event_name = 'page_view') AS pageviews,
  COUNT(DISTINCT user_pseudo_id) AS users,
  SUM(IFNULL(engagement_time_msec,0)) / 1000.0 AS total_engagement_time_sec,
  SAFE_DIVIDE(
    SUM(IFNULL(engagement_time_msec,0)) / 1000.0,
    NULLIF(COUNTIF(event_name = 'page_view'),0)
  ) AS avg_engagement_time_sec_per_pageview
FROM base
WHERE page_location IS NOT NULL
GROUP BY event_date, page_location, page_title
ORDER BY event_date, pageviews DESC;
