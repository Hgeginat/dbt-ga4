with events_base as (
    select 
        event_name,
        event_date_dt,
        user_pseudo_id,
        user_key,
        session_key,
        stream_id,
        (select value.string_value from unnest(event_params) where key = 'name') as name,
        (select value.string_value from unnest(event_params) where key = 'item_id') as item_id,
        (select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
        (select value.string_value from unnest(event_params) where key = 'item_category') as item_category,
        (select value.string_value from unnest(event_params) where key = 'content_type') as content_type,
    from {{ref('stg_ga4__events')}}
    where event_name ='select_content' or event_name = 'screen_name'
),
include_derived_session_properties as (
    select 
        events_base.*,
        session_properties.engagement_time_msec,
        session_properties.session_engaged
    from events_base
    {% if var('derived_session_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_session_properties')}} as session_properties using (session_key)
    {% endif %}
),

service_bookings as (
    select 
        event_name,
        event_date_dt as date,
        polestar_market,
        content_type,
        name,
        item_id,
        (case when engagement_time_msec > 0 or session_engaged = 1 then user_key else null end) as active_user_key,
        user_key,
        session_key
    from include_derived_session_properties 
    where ((LOWER(name) like 'app:you:polestarid:serviceandrepair' or LOWER(name) like 'app:you:polestarassistance' or lower(name) like 'app:you:polestarid:servicebooking') Or (content_type in ('polestar_assistance_call','servicing_date_start','servicing_time_start','servicing_error','servicing_complete','servicing_cancel_complete','servicing_book_click_enabled')))
)

select * from service_bookings
