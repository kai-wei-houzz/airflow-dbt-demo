SELECT
    order_id
FROM
    {{ source("c2_hourly", "order_attributes") }}
WHERE
    attribute_type = 91