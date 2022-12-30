with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category
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
        session_properties.session_engaged,
        session_properties.engagement_time_msec,
    from include_user_properties
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
error_events as (
    select 
        item_id,
        {{ga4.error_event_category('item_id')}} as error_event_category,
        event_date_dt as date,
        polestar_market,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        user_key,
        session_key
    from include_derived_session_properties 
    where LOWER(item_category) like 'app:carcontrol'
        and content_type = 'error_event'
)

select * from grouped_error_events
