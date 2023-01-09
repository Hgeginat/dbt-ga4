with screen_name_events2 as (
    select 
        event_date_dt as date,
        TIMESTAMP_MICROS(event_timestamp) as  time,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
    from {{ref('stg_ga4__events')}}
    where event_name = 'select_content'
),

include_derived_session_properties2 as (
    select 
        screen_name_events2.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from screen_name_events2 
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
Journey as (
    select date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        item_category,
        content_type,
        polestar_market,
        logged_in,
        is_paired,
       (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key
    
    from include_derived_session_properties2
    where logged_in in ('false','null')
)

select * from Journey