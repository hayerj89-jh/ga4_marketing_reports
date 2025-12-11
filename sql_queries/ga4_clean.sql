CREATE OR REPLACE TABLE `marketing_reports.core_web_page_performance` 
PARTITION BY event_date AS 

SELECT -- Core event-level
event_date,
event_timestamp,
event_name,
user_pseudo_id,
user_id,

-- Traffic source (user-scoped)
traffic_source.source        AS user_source,
traffic_source.medium        AS user_medium,
traffic_source.name          AS user_campaign,

-- Geo
geo.country,
geo.region,
geo.city,

-- Device / Tech
device.category              AS device_category,
device.operating_system      AS device_os,
device.browser               AS device_browser,

-- Page context (from event_params for page_view)
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'page_location')     AS page_location,
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'page_referrer')     AS page_referrer,
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'page_title')        AS page_title,

-- UTM / campaign parameters (from event_params)
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'source')            AS session_source,
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'medium')            AS session_medium,
(SELECT value.string_value FROM UNNEST(event_params)
 WHERE key = 'campaign')          AS session_campaign,

-- Session identifiers (from event_params)
(SELECT value.int_value FROM UNNEST(event_params)
 WHERE key = 'ga_session_id')     AS session_id,
(SELECT value.int_value FROM UNNEST(event_params)
 WHERE key = 'ga_session_number') AS session_number,

-- User first touch (lifetime)
user_first_touch_timestamp

FROM `marketing_data.analytics_448974598.events_*`
WHERE _TABLE_SUFFIX = '{{ ds_nodash }}' 