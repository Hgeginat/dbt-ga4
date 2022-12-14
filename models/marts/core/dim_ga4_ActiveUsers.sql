-- User dimensions: first geo, first device, last geo, last device, first seen, last seen

    select 
        user_key,
        min(event_timestamp) as first_seen_timestamp,
        min(event_date_dt) as first_seen_dt,
        max(event_timestamp) as last_seen_timestamp,
        max(event_date_dt) as last_seen_dt,
        count(distinct session_key) as num_sessions,
        sum(is_page_view) as num_page_views,
        sum(is_purchase) as num_purchases
    from {{ref('stg_ga4__events')}}
    
    where user_key is not null -- Remove users with privacy settings enabled
    AND ( session_engaged = 1 OR engagement_time_msec >0)

    
    group by 1