-- User Journey Funnel (session_start -> view_item -> add_to_cart -> purchase)
-- Creates: marketing_reports.user_journey_funnel_overview

#standardSQL
CREATE OR REPLACE TABLE `marketing_reports.user_journey_funnel_overview` 
PARTITION BY event_date AS
WITH base AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    (SELECT value.int_value
     FROM UNNEST(event_params)
     WHERE key = 'ga_session_id') AS ga_session_id,
    event_name
  FROM `marketing_data.analytics_448974598.events_*`
  WHERE _TABLE_SUFFIX = '{{ ds_nodash }}'
),
session_events AS (
  SELECT DISTINCT
    event_date,
    user_pseudo_id,
    ga_session_id,
    event_name
  FROM base
  WHERE ga_session_id IS NOT NULL
)
SELECT
  event_date,
  COUNT(DISTINCT IF(event_name = 'session_start',
                    CONCAT(user_pseudo_id, '-', ga_session_id),
                    NULL)) AS sessions_started,
  COUNT(DISTINCT IF(event_name = 'view_item',
                    CONCAT(user_pseudo_id, '-', ga_session_id),
                    NULL)) AS sessions_with_view_item,
  COUNT(DISTINCT IF(event_name = 'add_to_cart',
                    CONCAT(user_pseudo_id, '-', ga_session_id),
                    NULL)) AS sessions_with_add_to_cart,
  COUNT(DISTINCT IF(event_name = 'purchase',
                    CONCAT(user_pseudo_id, '-', ga_session_id),
                    NULL)) AS sessions_with_purchase
FROM session_events
GROUP BY event_date
ORDER BY event_date;
