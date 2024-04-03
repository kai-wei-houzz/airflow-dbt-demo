{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['dt'],
    pre_hook=["set hive.exec.dynamic.partition.mode=nonstrict", 'set spark.sql.sources.partitionOverwriteMode=DYNAMIC'],
  )
}}


WITH max_hour AS (
    SELECT
        max(HOUR(created)) max_hr
    FROM
        {{ source("c2_hourly", "orders") }}
    WHERE
        status IN (0, 1, 2, 3, 4, 5, 6, 20, 99, 101, 102)
    AND
        substring(created, 1, 10) = '{{ var("execution_date_pdt") }}'
)

SELECT
    to_date(created) AS created_date,
    hour(created) AS hr,
    CASE
        WHEN seller_type = 0 THEN '3rd party'
        WHEN seller_type = 5 THEN 'Private Label'
        ELSE 'Direct'
    END AS seller_types,
    acc_class,
    new_customer_flag,
    max_hr,
    l1_category_name,
    CASE
        WHEN lower(device_cat)= 'mweb' THEN 'mWeb'
        WHEN lower(device_cat)= 'dweb' THEN 'Dweb'
        WHEN lower(device_cat)= 'mobile' THEN 'App'
    END AS device_cat,
    count(item_id) AS items_cnt,
    count(DISTINCT user_id) AS customers,
    count(order_id) AS orders_cnt,
    sum(gmv) AS gmv,
    sum(direct_gmv) AS direct_gmv,
    sum(take_rate_3p) take_rate_3p ,
    sum(CASE WHEN seller_type <> 0 AND cop_before_allowance = 0 THEN -0.593 * gmv ELSE cop_before_allowance END) AS cop_before_allowance,
    sum(checkout_shipping) AS checkout_shipping, 
    "{{ var('execution_date_pdt') }}" AS dt
FROM (
    SELECT
        o.order_id,
        oi.item_id,
        max(oi.l1_category_name) AS l1_category_name,
        max(COALESCE(dc.device_cat, ov.device_cat)) AS device_cat,
        max(o.created) created,
        max(o.user_id) AS user_id,
        max(vi.seller_type) AS seller_type,
        max(CASE
                WHEN td.order_id IS NOT NULL THEN '20 - Trade'
                WHEN pro.user_id IS NOT NULL THEN '30 - Pro'
                ELSE '10 - Consumer'
            END) AS acc_class,
        max(CASE
                WHEN gc.order_id IS NOT NULL THEN 'guest'
                WHEN o.created = new_u.first_order_date THEN 'new'
                ELSE 'repeat'
            END) AS new_customer_flag,
        max(mx.max_hr) AS max_hr,
        max(-1.00 * COALESCE(oa.checkout_shipping, 0))/ max(num_items) AS checkout_shipping,
        sum(oi.price + oi.shipping) AS gmv,
        sum(CASE WHEN seller_type IN (1, 2, 3, 4) THEN oi.price + oi.shipping ELSE 0.00 END) AS direct_gmv,
        sum(CASE WHEN vi.seller_type = 0 THEN -1 *(1.00-0.18)*(oi.price + oi.shipping) ELSE 0.00 END) AS take_rate_3p,
        sum(-1.00 * COALESCE(oi.cost, 0)) AS cop_before_allowance
    FROM 
        {{ ref("orders") }} o
    INNER JOIN {{ ref("order_items") }} oi ON o.order_id = oi.order_id
    LEFT JOIN max_hour mx ON 1 = 1
    LEFT JOIN {{ source("c2_hourly", "vendor_info") }} vi ON vi.user_id = o.vendor_id
    LEFT JOIN {{ source("c2_hourly", "professionals") }} pro ON pro.user_id = o.user_id
    LEFT JOIN {{ ref("new_users") }} new_u ON o.user_id = new_u.user_id
    LEFT JOIN {{ ref("order_attributes") }} OA ON o.order_id = OA.order_id
    LEFT JOIN {{ ref("order_attributes_td") }} td ON td.order_id = o.order_id
    LEFT JOIN {{ ref("order_attributes_dc") }} dc ON o.order_id = dc.order_id
    LEFT JOIN {{ ref("order_attributes_gc") }} gc ON o.order_id = gc.order_id
    LEFT JOIN {{ ref("order_visitors") }} ov ON o.order_id = ov.order_id
    
    GROUP BY
        o.order_id,
        oi.item_id
)
GROUP BY
    to_date(created),
    HOUR(created),
    CASE
        WHEN seller_type = 0 THEN '3rd party'
        WHEN seller_type = 5 THEN 'Private Label'
        ELSE 'Direct'
    END,
    acc_class,
    new_customer_flag,
    max_hr,
    l1_category_name,
    device_cat
