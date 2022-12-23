with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key
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
include_derived_session_properties as (
    select 
        include_user_properties.*,
        session_properties.item_id,
        session_properties.item_category,
        session_properties.session_engaged,
        session_properties.engagement_time_msec,
        session_properties.content_type
    from include_user_properties
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
grouped_error_events as (
    select 
        item_id,
        {{ga4.error_event_category('item_id')}} as error_event_category,
        event_date_dt,
        polestar_market,
        count(distinct user_pseudo_id) as active_user_count,
        count(*) as error_count
    from include_derived_session_properties 
    where item_category = 'App:carcontrol'
        and content_type = 'error_event'
        and (session_engaged = 1 or engagement_time_msec > 0)
    group by item_id, error_event_category, event_date_dt, polestar_market
)

select * from grouped_error_events
