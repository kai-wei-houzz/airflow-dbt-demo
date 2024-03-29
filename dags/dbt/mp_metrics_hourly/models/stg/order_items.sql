    SELECT
        oip.order_id,
        oip.item_id,
        pcl.l1_category_name,
        oip.price,
        COALESCE((ois.shipping / oip.num_items), 0) AS shipping,
        oip.cost,
        oip.num_items
    FROM (
        SELECT
            order_id,
            item_id,
            price,
            cost,
            count(order_item_id) OVER(PARTITION BY order_id) AS num_items
        FROM
            {{ source("c2_hourly", "order_items") }}
        WHERE
            item_type = 1
        AND (
            to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 36)
            OR (
                to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 380)
                AND to_date(created) <= date_sub('{{ var("execution_date_pdt") }}', 364)
            )
        )
    ) oip
    LEFT JOIN (
        SELECT
            order_id,
            sum(shipping) AS shipping
        FROM
            {{ source("c2_hourly", "order_items") }}
        WHERE
            item_type IN (1, 2)
        AND (
            to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 36)
            OR (
                to_date(created) >= date_sub('{{ var("execution_date_pdt") }}', 380)
                AND to_date(created) <= date_sub('{{ var("execution_date_pdt") }}', 364)
            )
        )
        GROUP BY
            order_id
    ) ois ON oip.order_id = ois.order_id
    LEFT JOIN {{ source("c2_daily", "vendor_listings") }} vl ON oip.item_id = vl.vendor_listing_id
    LEFT JOIN {{ source("c2_daily", "houses") }} h ON vl.house_id = h.house_id
    LEFT JOIN (
        SELECT
            pc.category_id,
            cat1.name AS l1_category_name
        FROM
            {{ source("mp", "parent_category_lookup") }} pc
        LEFT JOIN
            c2.categories cat1 ON pc.level1 = cat1.category_id
    ) AS pcl ON h.category_id = pcl.category_id
