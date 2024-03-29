SELECT
    order_id,
    CASE
        WHEN user_agent LIKE '%com.houzz.app%'
            OR user_agent LIKE '%FBAN%' THEN 'App'
        WHEN user_agent LIKE '%iPad%' THEN 'mWeb'
        WHEN user_agent LIKE '%iPhone%' THEN 'mWeb'
        WHEN user_agent LIKE '%Android%' THEN 'mWeb'
        WHEN user_agent LIKE '%BlackBerry%' THEN 'mWeb'
        WHEN user_agent LIKE '%Windows Phone%' THEN 'mWeb'
        WHEN user_agent LIKE '%Media Center PC%' THEN 'Dweb'
        WHEN user_agent LIKE '%Macintosh; Intel Mac OS X%' THEN 'Dweb'
        WHEN user_agent LIKE '%MacBookPro%' THEN 'Dweb'
        WHEN user_agent LIKE '%Apple-PubSub%' THEN 'Dweb'
        WHEN user_agent LIKE '%Windows NT%' THEN 'Dweb'
        -- iPhone/iPad Houzz app user_agents include iPhone/iPad so remaining user_agents
        -- must be Android.
        WHEN user_agent LIKE '%Kindle%' THEN 'mWeb'
        ELSE 'other'
    END AS device_cat
FROM
    {{ source('static', 'order_visitors_static') }}
