with group1 as (
    select 
        session_key as session_key,
        event_date_dt as date,
        user_key as user_key,
        user_pseudo_id,
        device_operating_system,
        -- sum((engagement_time_msec))/1000 as engagement_time_sec,
        min (event_timestamp) as first_event_timestamp,
        max(event_timestamp) as last_event_timestamp,
        (max(event_timestamp)- min(event_timestamp))/1000000 as sessiontimesec

    from {{ref('stg_ga4__events')}} 
    group by 1,2,3,4, 5
   
),
-- select * from group1 where sessiontimesec > 0


include_user_properties_market as (
    select
     group1.*,
     user_properties.polestar_market,
     user_properties.logged_in,
     user_properties.is_paired
     from group1
     {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
)

select * from include_user_properties_market  where sessiontimesec > 0

