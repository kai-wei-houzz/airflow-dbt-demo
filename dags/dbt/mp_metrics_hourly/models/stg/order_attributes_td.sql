SELECT
    order_id
FROM
    {{ source("c2_hourly", "order_attributes")}}
WHERE
    attribute_type = 44
    OR (attribute_type = 25
        AND CAST(attribute_value AS tinyint) = 1
    )
GROUP BY
    order_id