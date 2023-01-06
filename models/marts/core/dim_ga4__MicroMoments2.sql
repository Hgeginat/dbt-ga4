with screen_name_events as (
    select 
        event_date_dt as date,
        user_key,
        stream_id, 
        user_pseudo_id,
        traffic_source_medium,
         (select value.string_value from unnest(event_params) where key = 'name') as name,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
    from {{ref('stg_ga4__events')}}
    where event_name = 'screen_name'
),

session_key_added as (
    select 
        screen_name_events.*,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as session_key,
        
        from screen_name_events
),

include_derived_session_properties as (
    select 
        session_key_added.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged 
    from session_key_added
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),

micro_moments as (
    select date,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        name,
        polestar_market,
        logged_in,
        is_paired,
        traffic_source_medium,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key, 
        engagement_time_msec,
        session_engaged,
      CASE WHEN name like 'App:Post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
           WHEN name like 'App:post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
      END as micro_moment_name
    from include_derived_session_properties
    where name like 'App:Post:%' or name like 'App:post:%'
)

select * from micro_moments