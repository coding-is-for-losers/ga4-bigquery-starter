{{ config(
    materialized='incremental',
    partition_by={
      "field": "event_date",
      "data_type": "date"},
    cluster_by= ["event_date", "ga_session_id"],
    incremental_strategy = 'insert_overwrite'
)}}

SELECT
parse_date("%Y%m%d", event_date) event_date,
event_timestamp,
user_pseudo_id,
user_first_touch_timestamp,
device.category	as device_category,
device.language	as device_language,
device.web_info.browser as device_browser,
geo.continent as geo_continent,
geo.country as geo_country,
max(if(params.key = 'ga_session_id', params.value.int_value, null)) ga_session_id,
max(if(params.key = 'ga_session_number', params.value.int_value, null)) ga_session_number,
cast(max(if(params.key = 'session_engaged', params.value.string_value, null)) as int64) session_engaged,
max(if(params.key = 'page_title', params.value.string_value, null)) page_title,
max(if(params.key = 'page_location', params.value.string_value, null)) page_location,
max(if(params.key = 'source', params.value.string_value, null)) utm_source,
max(if(params.key = 'medium', params.value.string_value, null)) utm_medium,
max(if(params.key = 'campaign', params.value.string_value, null)) utm_campaign,
max(if(params.key = 'page_referrer', params.value.string_value, null)) utm_referrer,
max(ecommerce.transaction_id) ecommerce_transaction_id,
max(ecommerce.purchase_revenue) ecommerce_purchase_revenue
FROM
`{{ target.project }}.{{ target.schema }}.events_*`,
UNNEST(event_params) AS params
WHERE event_name = 'page_view'

{% if is_incremental() %}
-- Refresh only recent session data to limit query costs, unless running with --full-refresh
	AND regexp_extract(_table_suffix,'[0-9]+') BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('session_lookback_days') }} DAY)) AND
  		FORMAT_DATE("%Y%m%d", CURRENT_DATE())
{% endif %}

GROUP BY event_date, event_timestamp, user_pseudo_id, user_first_touch_timestamp, device_category, device_language, device_browser, geo_continent, geo_country