with users_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        stream_id,
        engagement_time_msec,
        app_info_version,
        (select value.int_value from unnest(event_params) where key = 'engaged_session_event') as engaged_session_event,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_number') as ga_session_number,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(user_properties) where key = 'logged_in') as logged_in,
        (select value.string_value from unnest(user_properties) where key = 'is_paired') as is_paired,
        active_user_index,
        device_operating_system,
        geo_country

    from {{ref('base_ga4__events')}}
    
    ),

session_key_added as (
    select 
        users_base.*,
        -- to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as session_key,
        concat(user_pseudo_id, ga_session_id,  stream_id) as session_key,
        (case when engagement_time_msec > 0 or engaged_session_event = 1 then user_pseudo_id else null end) as active_user_key
        from users_base
),

final_table as (
    select
        event_date_dt,
        user_pseudo_id,
        active_user_index,
        active_user_key,
        session_key,
        CASE when event_name = 'first_open' THEN 1 ELSE 0 END as first_open,
        ga_session_id,
        ga_session_number,
        polestar_market,
        logged_in,
        is_paired,
        device_operating_system,
        app_info_version,
        geo_country
    from session_key_added)
   

select * from final_table