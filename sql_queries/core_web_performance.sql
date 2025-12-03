-- Core Web Daily Performance Overview
-- Creates: marketing_reports.core_web_daily_overview

#standardSQL
CREATE OR REPLACE TABLE `marketing_reports.core_web_daily_overview` 
PARTITION BY event_date AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_name,
    user_pseudo_id,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'engagement_time_msec') AS engagement_time_msec
  FROM `marketing_data.analytics_448974598.events_*`
  WHERE _TABLE_SUFFIX = '{{ ds_nodash }}' 
),
sessions AS (
  -- one row per session
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id
  FROM base
  WHERE event_name = 'session_start'
    AND ga_session_id IS NOT NULL
  GROUP BY event_date, user_pseudo_id, ga_session_id
),
engaged_sessions AS (
  -- sessions that had user_engagement event
  SELECT DISTINCT
    b.event_date,
    b.user_pseudo_id,
    b.ga_session_id
  FROM base b
  WHERE b.event_name = 'user_engagement'
    AND b.ga_session_id IS NOT NULL
),
session_engagement AS (
  -- total engagement time per session
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    SUM(IFNULL(engagement_time_msec,0)) / 1000.0 AS engagement_time_sec
  FROM base
  WHERE ga_session_id IS NOT NULL
  GROUP BY event_date, user_pseudo_id, ga_session_id
),
conversions AS (
  -- treat any event marked as conversion = 1 as a conversion
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    COUNT(*) AS conversions
  FROM (
    SELECT
      event_date,
      user_pseudo_id,
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'ga_session_id') AS ga_session_id,
      (SELECT value.int_value
       FROM UNNEST(event_params)
       WHERE key = 'is_conversion_event') AS is_conversion_event
    FROM `marketing_data.analytics_448974598.events_*`
    WHERE _TABLE_SUFFIX = '{{ ds_nodash }}'
  )
  WHERE ga_session_id IS NOT NULL
    AND is_conversion_event = 1
  GROUP BY event_date, user_pseudo_id, ga_session_id
)
SELECT
  s.event_date,
  COUNT(DISTINCT s.user_pseudo_id) AS users,
  COUNT(*) AS sessions,
  COUNT(DISTINCT es.ga_session_id) AS engaged_sessions,
  SAFE_DIVIDE(COUNT(DISTINCT es.ga_session_id), COUNT(*)) AS engaged_session_rate,
  AVG(se.engagement_time_sec) AS avg_engagement_time_sec_per_session,
  SUM(IFNULL(c.conversions,0)) AS total_conversions,
  SAFE_DIVIDE(SUM(IFNULL(c.conversions,0)), COUNT(*)) AS conversions_per_session
FROM sessions s
LEFT JOIN engaged_sessions es
  ON s.event_date = es.event_date
 AND s.user_pseudo_id = es.user_pseudo_id
 AND s.ga_session_id = es.ga_session_id
LEFT JOIN session_engagement se
  ON s.event_date = se.event_date
 AND s.user_pseudo_id = se.user_pseudo_id
 AND s.ga_session_id = se.ga_session_id
LEFT JOIN conversions c
  ON s.event_date = c.event_date
 AND s.user_pseudo_id = c.user_pseudo_id
 AND s.ga_session_id = c.ga_session_id
GROUP BY s.event_date
ORDER BY s.event_date;
