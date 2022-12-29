with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key,
        traffic_source_medium
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
        session_properties.name
    from include_user_properties
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),
micro_moments as (
    select 
        event_date_dt,
        polestar_market,
        traffic_source_medium,
        CASE WHEN name like 'App:Post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
            WHEN name like 'App:post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30) 
            end as micro_moment_name,
        case when (session_engaged = 1 or engagement_time_msec > 0) then user_key else null end as active_user_key,
        user_key,
        session_key
    from include_derived_session_properties 
    where (name like 'App:Post:%' or name like 'App:post:%') and
    event_name = 'screen_name'
)

select * from micro_moments
