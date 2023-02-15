WITH

purchase as (
    SELECT ----------- table shows user id, purchase rank, revenue, other user interaction information related with purchase
      user_pseudo_id,
      event_date as purchase_date,
      CAST(TIMESTAMP_SECONDS(CAST(event_timestamp/1000000 AS INT64)) AS TIME) as purchase_time,
      event_value_in_usd as revenue,
      RANK() OVER (PARTITION BY event_date, user_pseudo_id ORDER BY event_timestamp) as purchase_rank,
      category,
      browser,
      country
    FROM
      `tc-da-1.turing_data_analytics.raw_events`
    WHERE
      event_name = 'purchase' AND event_value_in_usd IS NOT NULL
),

first_event as (
    SELECT ---------- table shows user id, earliest event
      user_pseudo_id,
      event_date as first_event_date,
      MIN(CAST(TIMESTAMP_SECONDS(CAST(event_timestamp/1000000 AS INT64)) AS TIME)) as first_event_time
    FROM
      `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY
      1, 2
),

returned_user as ( ----------------------------------- table labels returned user and total revenue
  SELECT 
    user_pseudo_id,
    sum(event_value_in_usd) as revenue,
    max(p_rank) > 1 as label
  FROM
      (SELECT 
        user_pseudo_id,
        event_value_in_usd,
        RANK() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) as p_rank
      FROM
        `tc-da-1.turing_data_analytics.raw_events`
      WHERE
        event_name = 'purchase' AND event_value_in_usd IS NOT NULL)
  GROUP BY 
    1
),

final AS ( ------- tables shows date, user id, time spent, revenue generated, return label
SELECT 
    CAST(PARSE_DATE('%Y%m%d', CAST(purchase.purchase_date AS STRING)) AS DATE) purchase_date,
    purchase.user_pseudo_id,
    TIME_DIFF(purchase.purchase_time, first_event.first_event_time, MINUTE)/60 as hours_spent,
    purchase.revenue,
    purchase.category,
    purchase.country,
    returned_user.label
FROM
    purchase
JOIN first_event
    ON
    first_event.user_pseudo_id = purchase.user_pseudo_id
    AND
    first_event.first_event_date = purchase.purchase_date
    AND 
    purchase.purchase_rank = 1
JOIN 
    returned_user
    ON
    returned_user.user_pseudo_id = purchase.user_pseudo_id
ORDER BY
    purchase.purchase_date
)

SELECT *,     -----------------------final table with segmenting
  FROM 
       (
        SELECT *,
              --NTILE(10) OVER (ORDER BY hours_spent ) as score,
              CASE 
                WHEN hours_spent <= 1 THEN "Less than hour"
                WHEN hours_spent > 1 AND hours_spent <= 2 THEN "Between 1 and 2 hours"
                WHEN hours_spent > 2 AND hours_spent <= 3 THEN "Between 2 and 3 hours"
                WHEN hours_spent > 3 AND hours_spent <= 6 THEN "Between 3 and 6 hours"
                WHEN hours_spent > 6 THEN "More than 6 hours"
              END as manual_score
        FROM final
       )


