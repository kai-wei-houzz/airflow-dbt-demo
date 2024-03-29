SELECT
    user_id,
    min(created) first_order_date
FROM
    {{ source("c2_hourly", "orders")}}
WHERE
    status IN (0, 1, 2, 3, 4, 5, 6, 20, 99, 101)
GROUP BY
    user_id