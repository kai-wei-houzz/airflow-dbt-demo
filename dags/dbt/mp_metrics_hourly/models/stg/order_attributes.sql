SELECT
    order_id,
    sum(CAST(attribute_value AS double)) AS checkout_shipping
FROM
    {{ source("c2_hourly", "order_attributes") }}
WHERE
    attribute_type = 47
GROUP BY
    order_id