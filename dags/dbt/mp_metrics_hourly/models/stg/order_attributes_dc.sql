SELECT
    order_id,
    get_json_object(attribute_value, '$.platform') AS device_cat
FROM
    {{ source("c2_hourly", "order_attributes") }}
WHERE
    attribute_type = 100
