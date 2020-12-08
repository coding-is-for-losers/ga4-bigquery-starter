{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date"},
    cluster_by= ["event_date","user_pseudo_id","ga_session_id"],
    incremental_strategy = 'insert_overwrite'
)}}

SELECT
event_date,
event_timestamp,
user_pseudo_id,
user_first_touch_timestamp,
CASE WHEN first_value(event_timestamp) over paths = user_first_touch_timestamp THEN 'New'
	ELSE 'Returning' END as user_type,
ga_session_id,
ga_session_number,
session_engaged,
page_title,
page_location,
row_number() over paths as session_event_order,
first_value(page_location) over paths as first_path,
last_value(page_location) over paths as last_path,
lag(page_location) over paths as prev_path,
lead(page_location) over paths as next_path,
first_value(utm_source) over paths as utm_source,
first_value(utm_medium) over paths as utm_medium,
first_value(utm_campaign) over paths as utm_campaign,
first_value(utm_referrer) over paths as utm_referrer,
device_category,
device_language,
device_browser,
geo_continent,
geo_country,
ecommerce_transaction_id,
ecommerce_purchase_revenue
FROM {{ ref('pageviews_proc') }}
{% if is_incremental() %}
		
    -- Refresh only recent session data to limit query costs, unless running with --full-refresh
    WHERE event_date >= date_sub(current_date(), INTERVAL {{ var('session_lookback_days') }} DAY)
		
{% endif %}
WINDOW paths as (PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp asc)