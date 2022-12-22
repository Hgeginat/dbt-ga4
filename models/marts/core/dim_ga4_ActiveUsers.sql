with active_users as (
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
),
include_user_properties as (
    select 
        active_users.*,
        user_properties.logged_in,
        user_properties.is_paired,
        user_properties.polestar_market,
        user_properties.first_open_time
    from active_users
    {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
)

select * from include_user_properties
