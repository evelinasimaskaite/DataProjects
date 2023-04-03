select  
  orders.order_id,
  orders.customer_id,
  customers.customer_unique_id,
  customers.customer_state,
  case 
    when customers.customer_state  IN ('AP','AM', 'RR', 'AC', 'RO', 'TO', 'PA')  then "North"
    when customers.customer_state  IN ('BA','CE','PB','PE','AL', 'PI', 'RN', 'ES', 'MA') then "Northeast"
    when customers.customer_state  IN ('DF','GO','MS','MT') then "Central-West"
    when customers.customer_state  IN ('PR', 'RS', 'SC') then "South"
    when customers.customer_state  IN ('RJ','MG','SE','SP') then "Southeast"
  end as customer_region,
  case 
      when customer_state is not null then 'Brasil' 
  end as customer_country,
  coalesce(translations.string_field_1, 'other') AS product_category_name_tr,
  sellers.seller_state,
  case 
    when sellers.seller_state  IN ('AP','AM', 'RR', 'AC', 'RO', 'TO', 'PA')  then "North"
    when sellers.seller_state  IN ('BA','CE','PB','PE','AL', 'PI', 'RN', 'ES', 'MA') then "Northeast"
    when sellers.seller_state  IN ('DF','GO','MS','MT') then "Central-West"
    when sellers.seller_state  IN ('PR', 'RS', 'SC') then "South"
    when sellers.seller_state  IN ('RJ','MG','SE','SP') then "Southeast"
  end as seller_region,
  orders.order_status,
  order_items.seller_id,
  order_items.product_id,
  orders.order_purchase_timestamp,
  timestamp_diff(order_approved_at, order_purchase_timestamp, hour) as hours_to_approve,
  date_diff(order_delivered_carrier_date, order_purchase_timestamp, day) as days_to_carrier,
  date_diff(order_delivered_customer_date, order_purchase_timestamp, day) as days_to_customer,
  case when order_delivered_customer_date > order_estimated_delivery_date then date_diff(order_delivered_customer_date, order_estimated_delivery_date, day) else 0 end as delayed_to_customer_days,
  case when order_delivered_carrier_date > shipping_limit_date then date_diff(order_delivered_carrier_date, shipping_limit_date, day) else 0 end as delayed_to_carrier_days,
  date_diff(order_estimated_delivery_date, order_delivered_customer_date  , day) as days_estimated_vs_actual,
 
  order_items.order_items_total_value,
  order_items.order_items_value,
  order_items.freight_value,
  order_items.num_of_products,

  payments.pay_type_count,
  payments.payment_installments,
  payments.payment_value,

  coalesce(cast(round(review.review_score/review.review_count, 0) as string), 'no review') as avg_review_score
from 
  `tc-da-1.olist_db.olist_orders_dataset` orders
join
  (select  
    order_id,
    product_id,
    seller_id,
    shipping_limit_date,
    sum(price + freight_value) as order_items_total_value,
    sum(price) as order_items_value,
    sum(freight_value) as freight_value,
    count(order_item_id) as num_of_products
  from
    `tc-da-1.olist_db.olist_order_items_dataset` 
  group by
    1, 2, 3, 4) order_items 
on
  orders.order_id = order_items.order_id 
join 
  `tc-da-1.olist_db.olist_customesr_dataset` customers 
on 
  customers.customer_id = orders.customer_id
join 
  `tc-da-1.olist_db.olist_products_dataset` products 
on 
  products.product_id = order_items.product_id
left join
  `tc-da-1.olist_db.product_category_name_translation` translations 
on  
  products.product_category_name = translations.string_field_0
join
  `tc-da-1.olist_db.olist_sellers_dataset` sellers 
on
  sellers.seller_id = order_items.seller_id
join
  (select
    order_id,
    count(payment_installments) as payment_installments,
    count(distinct payment_type) as pay_type_count,
    sum(payment_value) as payment_value
  from
    `tc-da-1.olist_db.olist_order_payments_dataset` group by 1) payments  
on
  orders.order_id = payments.order_id
left join
  (select
    order_id,
    sum(review_score) review_score,
    count(*) review_count,
    min(review_answer_timestamp) as review_date
  from
  `tc-da-1.olist_db.olist_order_reviews_dataset`
  group by 1) review     
on
  orders.order_id = review.order_id

