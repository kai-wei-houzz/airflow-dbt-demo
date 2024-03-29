SELECT
    -- *
    order_id,
    user_id,
    vendor_id,
    created
FROM
    {{ source('c2_hourly', 'orders') }}
WHERE
    status IN (0, 1, 2, 3, 4, 5, 6, 20, 99, 101)
AND ( 
    to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 36)
    OR (
        to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 380)
        AND to_date(created) <= date_sub('{{ var("execution_date_pdt") }}', 364)
    )
)