-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)
-- merging here intraday data with total data, but we do not have intraday data
with base_events as (
    select * from {{ ref('base_ga4__events')}}
    {% if var('frequency', 'daily') == 'daily+streaming' %}
    union all
    select * from {{ref('base_ga4__events_intraday')}}
    {% endif %}
),
-- Add a unique key for the user that checks for user_id and then pseudo_user_id
add_user_key as (
    select 
        *,
        case
            when user_id is not null then to_base64(md5(user_id))
            when user_pseudo_id is not null and user_pseudo_id != '' then to_base64(md5(user_pseudo_id))
            else null -- this case is reached when privacy settings are enabled and (possibly) for certain audience triggered events
        end as user_key
    from base_events
), 

-- Add unique key for sessions. session_key will be null if user_pseudo_id is null due to consent being denied. ga_session_id may be null during audience trigger events. 
include_session_key as (
    select 
        *,
        to_base64(md5(CONCAT(stream_id, user_pseudo_id, CAST(ga_session_id as STRING)))) as session_key -- Surrogate key to determine unique session across streams and users. Sessions do NOT reset after midnight in GA4
    from add_user_key
),
-- Add a key that combines session key and date. Useful when working with session table within date-partitioned tables
include_session_partition_key as (
    select 
        *,
        CONCAT(session_key, CAST(event_date_dt as STRING)) as session_partition_key
    from include_session_key
),
-- Add unique key for events
include_event_key as (
    select 
        *,
        to_base64(md5(CONCAT(session_key, event_name, CAST(event_timestamp as STRING), to_json_string(event_params)))) as event_key -- Surrogate key for unique events.  
    from include_session_partition_key
)
-- Remove specific query strings from page_location field

select * from include_event_key