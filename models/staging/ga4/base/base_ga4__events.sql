-- This checks if the variable static_incremental_days is set and is not false. If it's true, it means that incremental loading with specific partitioning is enabled.
-- If incremental loading is enabled, this loop generates a list of partition values to be replaced. 
-- It iterates static_incremental_days times and constructs a list of SQL expressions representing dates in the past, using date_sub to subtract days from the current date.

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
                "field": "event_date_dt",
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

-- Defined import mode done

-- Selecting the datapoints

with source as (
    select 
        parse_date('%Y%m%d',event_date) as event_date_dt,  -- event_date field from a string format (YYYYMMDD)
        event_timestamp,
        event_name,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo, -- to know this select all variable related to geo ( ie: geo_continent)
        app_info,
        traffic_source,
        stream_id,
        platform,
        ecommerce,
        items,
        collected_traffic_source.manual_campaign_id AS `collected_traffic_source.manual_campaign_id`,
        collected_traffic_source.manual_campaign_name AS `collected_traffic_source.manual_campaign_name`,
        collected_traffic_source.manual_source AS `collected_traffic_source.manual_source`,
        collected_traffic_source.manual_medium AS `collected_traffic_source.manual_medium`,
        collected_traffic_source.manual_term AS `collected_traffic_source.manual_term`,
        collected_traffic_source.manual_content AS `collected_traffic_source.manual_content`,
        collected_traffic_source.gclid AS `collected_traffic_source.gclid`,
        collected_traffic_source.dclid AS `collected_traffic_source.dclid`,
        collected_traffic_source.srsltid AS `collected_traffic_source.srsltid`,
        is_active_user,
        -- Determines the source table dynamically based on conditions.If the variable frequency is set to 'streaming', it selects from a table named events_intraday. 
        --Otherwise, it selects from a table named events but excludes any tables containing 'intraday' in their name.

    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source('ga4', 'events_intraday') }}
        where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% else %}
        from {{ source('ga4', 'events') }}
        where _table_suffix not like '%intraday%'
        and cast( _table_suffix as int64) >= {{var('start_date')}}
    {% endif %}

    --  from `polestar-explore`.`analytics_200752076`.`events_*`
    --   where _table_suffix not like '%intraday%' and cast( _table_suffix as int64) >= 20200820

    -- If the process is incremental (i.e., is_incremental() returns true).
    -- If static_incremental_days variable  is provided, it filters data based on a list of specific dates (partitions_to_replace) (see above)
    -- Otherwise, it filters data based on the maximum event date (_dbt_max_partition) already processed, ensuring only new data is included.
    {% if is_incremental() %}

        {% if var('static_incremental_days', false ) %}
            and parse_date('%Y%m%d', _TABLE_SUFFIX) in ({{ partitions_to_replace | join(',') }})
        {% else %}
            and parse_date('%Y%m%d',_TABLE_SUFFIX) >= _dbt_max_partition
        {% endif %}
    {% endif %}
),


renamed as (
    select 
        event_date_dt,
        event_timestamp,
        lower(replace(trim(event_name), " ", "_")) as event_name, -- Clean up all event names to be snake cased
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info.analytics_storage as privacy_info_analytics_storage,
        privacy_info.ads_storage as privacy_info_ads_storage,
        privacy_info.uses_transient_token as privacy_info_uses_transient_token,
        user_properties,
        user_first_touch_timestamp,
        user_ltv.revenue as user_ltv_revenue,
        user_ltv.currency as user_ltv_currency,
        device.category as device_category,
        device.mobile_brand_name as device_mobile_brand_name,
        device.mobile_model_name as device_mobile_model_name,
        device.mobile_marketing_name as device_mobile_marketing_name,
        device.mobile_os_hardware_model as device_mobile_os_hardware_model,
        device.operating_system as device_operating_system,
        device.operating_system_version as device_operating_system_version,
        device.vendor_id as device_vendor_id,
        device.advertising_id as device_advertising_id,
        device.language as device_language,
        device.is_limited_ad_tracking as device_is_limited_ad_tracking,
        device.time_zone_offset_seconds as device_time_zone_offset_seconds,
        device.browser as device_browser,
        device.browser_version as device_browser_version,
        device.web_info.browser as device_web_info_browser,
        device.web_info.browser_version as device_web_info_browser_version,
        device.web_info.hostname as device_web_info_hostname,
        geo.continent as geo_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.sub_continent as geo_sub_continent,
        geo.metro as geo_metro,
        app_info.id as app_info_id,
        app_info.version as app_info_version,
        app_info.install_store as app_info_install_store,
        app_info.firebase_app_id as app_info_firebase_app_id,
        app_info.install_source as app_info_install_source,
        traffic_source.name as traffic_source_name,
        traffic_source.medium as traffic_source_medium,
        traffic_source.source as traffic_source_source,
        stream_id,
        platform,
        ecommerce,
        items,
        `collected_traffic_source.manual_campaign_id`,
        `collected_traffic_source.manual_campaign_name`,
        `collected_traffic_source.manual_source`,
        `collected_traffic_source.manual_medium`,
        `collected_traffic_source.manual_term`,
        `collected_traffic_source.manual_content`,
        `collected_traffic_source.gclid`,
        `collected_traffic_source.dclid`,
        `collected_traffic_source.srsltid`,
        (case when (is_active_user = true) then 1 else 0 end) as active_user_index,
        {{ ga4.unnest_key('event_params', 'ga_session_id', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'ga_session_number',  'int_value') }},
        (case when (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" then 1 end) as session_engaged,
        {{ ga4.unnest_key('event_params', 'engagement_time_msec', 'int_value') }}
       
        
    from source 
)

select * from renamed
qualify row_number() over(partition by event_date_dt, stream_id, user_pseudo_id, ga_session_id, event_name, event_timestamp, to_json_string(event_params)) = 1 
--It partitions the data by several columns (event_date_dt, stream_id, user_pseudo_id, etc.) and retains only the first row for each unique combination of these columns.
-- This helps remove duplicate or redundant rows from the dataset.
