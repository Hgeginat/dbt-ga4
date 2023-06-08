
with start as (
    select 
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as Session_key,
        stream_id, 
        ga_session_id,
        ga_session_number,
        user_pseudo_id,
        app_info_version,
        event_date_dt as date,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        ((select value.int_value from unnest(event_params) where key = 'engaged_session_event')) as session_engaged,
        ((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engagement_time_seconds,

    from {{ref('stg_ga4__events')}}
    where event_name="user_engagement"
   
    
)

select * from start 

















