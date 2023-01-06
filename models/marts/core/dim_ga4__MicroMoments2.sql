with screen_name_events as (
    select 
        event_date_dt as date,
        user_key,
        stream_id, 
        user_pseudo_id,
        traffic_source_medium,
        engagement_time_msec,
        (select value.int_value from unnest(event_params) where key = 'engaged_session_event') as engaged_session_event,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number,
        (select value.string_value from unnest(event_params) where key = 'name') as name,
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
        (case when engagement_time_msec > 0 or engaged_session_event = 1 then user_pseudo_id else null end) as active_user_key
        from screen_name_events
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
        active_user_key,
      CASE WHEN name like 'App:Post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
           WHEN name like 'App:post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
      END as micro_moment_name
    from session_key_added
    where name like 'App:Post:%' or name like 'App:post:%'
)

select * from micro_moments