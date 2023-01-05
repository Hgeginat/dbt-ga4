with screen_name_events2 as (
    select 
        event_date_dt as date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type
    from {{ref('stg_ga4__events')}}
    where event_name = 'select_content'
),
include_user_properties2 as (
    select 
        screen_name_events2.*,
        user_properties.polestar_market,
        user_properties.logged_in,
        user_properties.is_paired

    from screen_name_events2
    {% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} as user_properties using (user_key)
    {% endif %}
),
include_derived_session_properties2 as (
    select 
        include_user_properties2.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from include_user_properties2
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