-- looking into dbt_project to see if the variable exist

{{ config(
  enabled = true if var('derived_session_properties', false) else false,
  materialized = "table"
) }}

-- Remove null session_keys (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where session_key is not null
),

-- unnest all user properties & event params to unest based on information in dbt_project file
unnest_event_params as
(
    select 
        session_key,
        event_timestamp
        {% for sp in var('derived_session_properties', []) %}
            {% if sp.user_property %}
                , {{ ga4.unnest_key('user_properties', sp.user_property, sp.value_type) }}
            {% else %}
                , {{ ga4.unnest_key('event_params', sp.event_parameter, sp.value_type) }}
            {% endif %}
        {% endfor %}
    from events_from_valid_users


)

--After unnesting, it selects distinct session keys and applies a window function to calculate the last value of each session property over a session window.
 -- It iterates over the properties specified in derived_session_properties, applying the window function accordingly.
 
SELECT DISTINCT
    session_key
    {% for sp in var('derived_session_properties', []) %}
        , LAST_VALUE({{ sp.user_property | default(sp.event_parameter) }} IGNORE NULLS) OVER (session_window) AS {{ sp.session_property_name }}
    {% endfor %}
FROM unnest_event_params
WINDOW session_window AS (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
