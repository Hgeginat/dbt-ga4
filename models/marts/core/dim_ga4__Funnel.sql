with users_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        stream_id,
        engagement_time_msec,
        (select value.int_value from unnest(event_params) where key = 'engaged_session_event') as engaged_session_event,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired
        

    from {{ref('stg_ga4__events')}}
    ),

session_key_added as (
    select 
        users_base.*,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as session_key,
        (case when engagement_time_msec > 0 or engaged_session_event = 1 then user_pseudo_id else null end) as active_user_key
        from users_base
),

final_table as (
    select
        event_date_dt,
        user_pseudo_id,
        active_user_key,
        polestar_market,
        logged_in,
        is_paired
    from session_key_added)

select * from final_table