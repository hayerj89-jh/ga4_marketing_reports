-- Marketing Acquisition Overview (sessions by source / medium / campaign)
-- Creates: marketing_reports.marketing_acquisition_sessions

#standardSQL
CREATE OR REPLACE TABLE `marketing_reports.marketing_acquisition_sessions` 
PARTITION BY event_date AS
WITH session_starts AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'session_source') AS session_source,
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'session_medium') AS session_medium,
    (SELECT value.string_value
     FROM UNNEST(event_params)
     WHERE key = 'session_campaign') AS session_campaign
  FROM `marketing_data.analytics_448974598.events_*`
  WHERE event_name = 'session_start'
    AND _TABLE_SUFFIX = '{{ ds_nodash }}'
)
SELECT
  event_date,
  COALESCE(session_source, '(not set)') AS source,
  COALESCE(session_medium, '(not set)') AS medium,
  COALESCE(session_campaign, '(not set)') AS campaign,
  COUNT(DISTINCT CONCAT(user_pseudo_id, '-', ga_session_id)) AS sessions
FROM session_starts
GROUP BY event_date, source, medium, campaign
ORDER BY event_date, sessions DESC;
