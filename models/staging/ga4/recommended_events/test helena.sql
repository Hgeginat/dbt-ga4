-- copy of stg__ga4__events.sql to see on how to add Polestarmarket

-- This staging model contains key creation and window functions. Keeping window functions outside of the base incremental model ensures that the incremental updates don't artificially limit the window partition sizes (ex: if a session spans 2 days, but only 1 day is in the incremental update)

 with base_events as (
    select * from {{ ref('base_ga4__events')}}
    
)

select * from base_events