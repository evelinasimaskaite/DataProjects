WITH 
main_table AS (
SELECT
  user_pseudo_id,
  DATE_TRUNC(CAST(PARSE_DATE('%Y%m%d', CAST(MIN(event_date) OVER(PARTITION BY user_pseudo_id) AS STRING)) AS DATE), WEEK) as first_event_week,
  DATE_TRUNC(CAST(PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS DATE), WEEK) as event_date,
  event_value_in_usd as event_value
FROM
  `tc-da-1.turing_data_analytics.raw_events`
WHERE 
  CAST(PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS DATE) BETWEEN '2020-11-01' AND '2021-01-30'
)

SELECT
  first_event_week as registration_week,
  COUNT(DISTINCT user_pseudo_id) as customers,

  SUM(CASE WHEN event_date = first_event_week THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_0, --------------------- total revenue generated in this week by the cohort / cohort size (customers)

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 1 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_1, 

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 2 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_2, 

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 3 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_3,

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 4 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_4,

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 5 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_5,

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 6 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_6,   

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 7 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_7, 

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 8 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_8,  

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 9 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_9,   

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 10 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_10, 

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 11 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_11,  

  SUM(CASE WHEN DATE_ADD(first_event_week, INTERVAL 12 WEEK) = event_date THEN event_value END) / 
    COUNT(DISTINCT user_pseudo_id) as week_12

FROM
  main_table
GROUP BY
  1
ORDER BY 
  1
;