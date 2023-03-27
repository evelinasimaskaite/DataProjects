WITH 
main_table AS  ------------ main fields needed for the analysis
(
SELECT
  user_pseudo_id as user_id,
  event_name,
  CAST(PARSE_DATE('%Y%m%d', event_date)  AS DATE) as event_date,
  TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
  event_value_in_usd,
  category,
  medium,
  campaign
FROM 
  `tc-da-1.turing_data_analytics.raw_events` 
),

new_session_def AS  ----------------- Session definition and session starts
(
SELECT 
  *, 
  CASE 
    WHEN DATETIME_DIFF(event_timestamp, LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp),SECOND) > 1800 -- pirmas eventas po ilgos pauzes
    OR DATE_DIFF(CAST(event_timestamp AS DATE), CAST(COALESCE(LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp)) AS DATE), DAY) > 0  -- pirmas eventas po vidurnakcio
    OR LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp) IS NULL -- pirmas eventas apskritai
    THEN event_timestamp
    ELSE NULL
  END AS session_start
FROM 
    main_table 
),
sessionized_table AS ----------------- assign session ID
(
SELECT     
   CONCAT(user_id,"-", LAST_VALUE(session_start IGNORE NULLS)
    OVER (PARTITION BY user_id ORDER BY event_timestamp ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) as session_id, 
   event_timestamp,
   FORMAT_DATE('%u', event_timestamp) week_day,
   user_id,
   event_name,
   campaign,
   category,
   medium, 
   event_value_in_usd
FROM new_session_def     
ORDER BY user_id 
)
 ,
 session_duration AS  
 (
SELECT        ------------- get session duration
  session_id,
  DATETIME_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS duration
FROM
  sessionized_table
GROUP BY 
  session_id
)

SELECT
  CASE 
       WHEN Campaign IN ('Data Share Promo','NewYear_V1','BlackFriday_V1','NewYear_V2','BlackFriday_V2','Holiday_V2','Holiday_V1','(data deleted)') THEN 'Paid Media' 
       WHEN (Campaign IN ('(referral)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(referral)') THEN 'Referral' 
       WHEN (Campaign IN ('(organic)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(organic)') THEN 'Organic' 
       WHEN (Campaign IN ('(direct)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(direct)') THEN 'Direct'
       WHEN (Campaign IN ('<Other>') AND medium IN ('cpc')) THEN 'Paid search' 
       WHEN Campaign IN ('<Other>') Then 'Other' 
       END AS Campaign_type,
  CASE 
       WHEN Campaign IN ('Data Share Promo','NewYear_V1','BlackFriday_V1','NewYear_V2','BlackFriday_V2','Holiday_V2','Holiday_V1','(data deleted)') THEN Campaign  
       WHEN (Campaign IN ('(referral)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(referral)') THEN 'Non-Campaign' 
       WHEN (Campaign IN ('(organic)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(organic)') THEN 'Non-Campaign' 
       WHEN (Campaign IN ('(direct)') AND medium IN ('cpc')) THEN 'Paid search'
       WHEN Campaign IN ('(direct)') THEN 'Non-Campaign'
       WHEN (Campaign IN ('<Other>') AND medium IN ('cpc')) THEN 'Paid search' 
       WHEN Campaign IN ('<Other>') Then 'Non-Campaign' 
       END AS Campaign_name,
  sessionized_table.session_id,
  CAST(sessionized_table.event_timestamp AS DATE) as event_date,
  sessionized_table.user_id,
  sessionized_table.event_name,
  sessionized_table.campaign,
  sessionized_table.category,
  sessionized_table.medium,
  sessionized_table.event_value_in_usd,
  session_duration.duration,
  ROW_NUMBER() OVER (PARTITION BY sessionized_table.session_id ORDER BY sessionized_table.event_timestamp) as event_rank,
  COUNT(CASE WHEN event_name = "page_view" THEN sessionized_table.event_name END) OVER (PARTITION BY sessionized_table.session_id) as pageviews_per_session,
  COUNT(sessionized_table.session_id) OVER (PARTITION BY sessionized_table.session_id) as event_per_session_count
FROM
  sessionized_table
JOIN
  session_duration
ON
  sessionized_table.session_id = session_duration.session_id
-- WHERE 
--   campaign IS NOT NULL
ORDER BY 
  session_id, event_rank
