with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key,
        stream_id,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
    from {{ref('stg_ga4__events')}}
),
-- add_session_properties as (
--     select 
--         events_base.*,
--         session_properties.engagement_time_msec,
--         session_properties.session_engaged
--     from 
--         events_base 
--     left join (
--         select 
--            max((select value.int_value from unnest(event_params) where key = 'engagement_time_msec')) as engagement_time_msec,
--            sum((select value.int_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
--            session_key
--         from {{ref('stg_ga4__events')}}
--         where (select value.int_value from unnest(event_params) where key = 'engagement_time_msec') is not null and
--             (select value.int_value from unnest(event_params) where key = 'session_engaged') is not null and
--             session_key is not null
--         group by session_key
--     ) as session_properties using (session_key)
-- ),
include_derived_session_properties as (
    select 
        events_base.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from events_base
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
grouped_app_events as (
    select 
        event_name,
        content_type,
        item_category,
        {{ga4.car_control_category('content_type')}} as car_control_category,
        event_date_dt,
        polestar_market,
        count(distinct case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_count,
        count(distinct user_key) as user_count,
        count(distinct session_key) as session_count
    from include_derived_session_properties 
    where LOWER(item_category) like 'app:carcontrol%'
    group by event_name, content_type, item_category, event_date_dt, polestar_market, car_control_category
)

select * from grouped_app_events 
