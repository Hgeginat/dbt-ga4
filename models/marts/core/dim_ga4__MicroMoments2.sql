with screen_name_events as (
    select 
        event_date_dt,
        user_key,
        ga_session_id,
        stream_id, 
        user_pseudo_id,
        session_key,
        (select value.string_value from unnest(event_params) where key = 'name') as name
    from {{ref('stg_ga4__events')}}
    where event_name = 'screen_name'
),
micro_moments as (
    select screen_name_events.*,
      CASE WHEN name like 'App:Post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
           WHEN name like 'App:post:%' THEN SUBSTR(REGEXP_REPLACE(name, 'App:(Post|post):[0-9]*:', ''),1,30)
      END as micro_moment_name
    from screen_name_events
    where name like 'App:Post:%' or name like 'App:post:%'
)

select * from micro_moments