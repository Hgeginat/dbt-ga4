
with start as (
    select 
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING),CAST(ga_session_number as STRING)))) as Session_key,
        stream_id, 
        ga_session_id,
        ga_session_number,
        user_pseudo_id,
        event_date_dt as date,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        max((select value.int_value from unnest(event_params) where key = 'session_engaged')) as session_engaged,
        max((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engagement_time_seconds,

    from {{ref('stg_ga4__events')}}
    group by  
    1,2,3,4,5,6,7,8,9
    
)

select * from start where Session_key is not null

















