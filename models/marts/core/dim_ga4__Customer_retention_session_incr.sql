{% if var('static_incremental_days', false ) %} -- if true, this part of the code will be running only if the variable static_incremental_days exist. if it does not exist, the default value of it will be false. Therefore, the code in the block will not run
    {% set partitions_to_replace = [] %} --  Initializes an empty list named partitions_to_replace aim :  This list is intended to store partition values for incremental loading.

    {% for i in range(var('static_incremental_days')) %}   -- loop for 0 to incrementalsday-1
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %} -- getting a list of date where we remove the amount of days from 0 to incrementals days-1
    {% endfor %}


 --  config : This function generates configurations for the data loading process. so the first part here is configuring the upload 
    {{
        config(                             
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "date",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}

--meaning her no 'static_incremental_days' value, and therefore incremental update over here
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}



with first_part_data as (

select 
event_date_dt,
user_key,
session_key,
(select value.string_value from unnest(user_properties) where key = 'polestar_market') as polestar_market,
device_operating_system

from {{ref('stg_ga4__events')}}

{% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',')}})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
),

second_part_data as (

select 
event_date_dt as date,
user_key,
polestar_market,
device_operating_system,
COUNT(DISTINCT session_key) as amount_sessions

from first_part_data
group by 1,2,3,4)

select * from second_part_data where (amount_sessions >0)
   