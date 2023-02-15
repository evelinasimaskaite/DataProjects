SELECT
  DATE_TRUNC(subscription_start, WEEK) AS registration_week,
  COUNT(user_pseudo_id) AS cohort_user_count,
  COUNT(user_pseudo_id) AS week_0,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 1 WEEK) THEN user_pseudo_id END) AS week_1,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 2 WEEK) THEN user_pseudo_id END) AS week_2,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 3 WEEK) THEN user_pseudo_id END) AS week_3,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 4 WEEK) THEN user_pseudo_id END) AS week_4,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 5 WEEK) THEN user_pseudo_id END) AS week_5,
  COUNT(CASE WHEN (COALESCE(subscription_end, CURRENT_DATE())) > DATE_ADD(DATE_TRUNC(subscription_start, WEEK), INTERVAL 6 WEEK) THEN user_pseudo_id END) AS week_6
FROM
  `tc-da-1.turing_data_analytics.subscriptions`
GROUP BY
  1;