with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key,
        stream_id
    from {{ref('stg_ga4__events')}}
),
include_user_properties as (
    select 
        events_base.*,
        user_properties.polestar_market
    from events_base
    {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
),
-- include_derived_user_properties as (
--     select 
--         include_user_properties.*,
--         aggregated_derived_user_properties.engagement_time_msec,
--         aggregated_derived_user_properties.session_engaged
--     from include_user_properties
--     inner join (
--         select user_key,
--             sum(derived_user_properties.user_engagement_time_msec) as engagement_time_msec,
--             sum(derived_user_properties.user_session_engaged) as session_engaged
--         from events_base
--         {% if var('derived_user_properties', false) %}
--         -- If derived user properties have been assigned as variables, join them on the user_key
--         left join {{ref('stg_ga4__derived_user_properties')}} as derived_user_properties using (user_key)
--         {% endif %}
--         group by user_key
--     ) as aggregated_derived_user_properties using (user_key)
-- ),
include_derived_session_properties as (
    select 
        include_user_properties.*,
        session_properties.item_category,
        session_properties.content_type,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from include_user_properties
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
        count(distinct case when engagement_time_msec > 0 then user_pseudo_id else null end) as active_user_count_time,
        count(distinct case when session_engaged = 1      then user_pseudo_id else null end) as active_user_count_engaged,
        count(distinct case when engagement_time_msec > 0 or session_engaged = 1 then user_pseudo_id else null end) as active_user_count,
        count(distinct session_key) as session_count
    from include_derived_session_properties 
    where LOWER(item_category) like 'app:carcontrol%' and
        (stream_id = '1462390642' or stream_id = '1462846118')
    group by event_name, content_type, item_category, event_date_dt, polestar_market, car_control_category
)

select * from grouped_app_events 
