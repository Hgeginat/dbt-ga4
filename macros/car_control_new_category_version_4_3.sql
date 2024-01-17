
{% macro car_control_new_category_level_3(content_type, item_id, item_category) %}
CASE
    WHEN (TRIM(content_type) ='modal' AND TRIM (item_id) = 'airquality_information_click' AND TRIM (item_category) = 'App:carcontrol:airquality' ) THEN 'Info'
    WHEN (TRIM(content_type) ='invocation_error' AND TRIM (item_id) = 'tile_overview_invocation_error' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Tile invocation error'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'charge_ampere_minus_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Amp -'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'charge_ampere_plus_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Amp +'
    WHEN (TRIM(content_type) ='modal' AND TRIM (item_id) = 'charge_ampere_information_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Amp info '
    WHEN (TRIM(content_type) ='battery_details' AND TRIM (item_id) = 'collapse' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Close'
    WHEN (TRIM(content_type) ='battery_details' AND TRIM (item_id) = 'expand' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Open'
    WHEN (TRIM(content_type) ='nav_click' AND TRIM (item_id) = 'charge_schedule_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Schedule '
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'charge_schedule_done_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Schedule Done '
    WHEN (TRIM(content_type) ='modal' AND TRIM (item_id) = 'charge_schedule_information_click' AND TRIM (item_category) = 'App:carcontrol:charge' ) THEN 'Schedule info '
    WHEN (TRIM(content_type) ='amps_amperage_limit_unavailable_error' AND TRIM (item_id) = 'undocumented type' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Amp limit not available'
    WHEN (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_amp_error_get' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Unable to fetch Amp '
    WHEN (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_timeout_fetch_battery_status' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Unable to fetch Battery status'
    WHEN (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_battery_charge_off' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Off'
    WHEN (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_battery_charge_on' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'On'
    WHEN (TRIM(content_type) ='modal' AND TRIM (item_id) = 'climate_information_click' AND TRIM (item_category) = 'App:carcontrol:climate' ) THEN 'Climate information'
    WHEN (TRIM(content_type) ='add_timer_click' AND TRIM (item_id) = 'null' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Adding timer'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_new_timer' AND TRIM (item_category) = 'App:carcontrol:climate' ) THEN 'New Timer'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_close_timer' AND TRIM (item_category) = 'App:carcontrol:climate' ) THEN 'Close Timer'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_delete_timer' AND TRIM (item_category) = 'App:carcontrol:climate' ) THEN 'Delete Timer'
    WHEN (TRIM(content_type) ='save_timer' AND TRIM (item_id) = 'no_repeat' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'No Repeat'
    WHEN (TRIM(content_type) ='save_timer' AND TRIM (item_id) = 'repeat' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Repeat Weekly'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_save_timer' AND TRIM (item_category) = 'App:carcontrol:climate' ) THEN 'Save Timer'
    WHEN (TRIM(content_type) ='charging_timer_error_sync' AND TRIM (item_id) = 'undocumented type' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Unable to charge Timer'
    WHEN (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_timeout_fetch_climate_status' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Unable to fetch Climate status'
    WHEN (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_not_able_to_stop_climate' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Unable to stop climate'
    WHEN (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_timeout_lock_doors' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Timeout'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'position_copy_address_click' AND TRIM (item_category) = 'App:carcontrol:position' ) THEN 'Copy address'
    WHEN (TRIM(content_type) ='nav_click' AND TRIM (item_id) = 'position_open_in_navigation_app_click' AND TRIM (item_category) = 'App:carcontrol:position' ) THEN 'Open in navigation app'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'position_show_car_pos_click' AND TRIM (item_category) = 'App:carcontrol:position' ) THEN 'Show car location'
    WHEN (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'position_show_phone_pos_click' AND TRIM (item_category) = 'App:carcontrol:position' ) THEN 'Show phone location '
    WHEN (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_position_click' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'Setting position click'
    WHEN (TRIM(content_type) ='page_view' AND TRIM (item_id) = 'tile_overview_page_view' AND TRIM (item_category) = 'App:carcontrol' ) THEN 'View'

    WHEN
    (TRIM(content_type) ='edit_timer' AND TRIM (item_id) = 'null' AND TRIM (item_category) = 'App:carcontrol' ) OR 
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_edit_timer' AND TRIM (item_category) = 'App:carcontrol:climate' ) 
    THEN 'Edit Timer'

    WHEN
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'doors_lock' AND TRIM (item_category) = 'App:carcontrol:doors' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_doors_lock' AND TRIM (item_category) = 'App:carcontrol' ) 
    THEN 'Lock'

    WHEN
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'doors_unlock' AND TRIM (item_category) = 'App:carcontrol:doors' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_doors_unlock' AND TRIM (item_category) = 'App:carcontrol' ) 
 
    THEN 'Unlock'

    WHEN
    (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_timeout_fetch_door_status' AND TRIM (item_category) = 'App:carcontrol' ) OR 
    (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_not_able_to_get_status_of_doors' AND TRIM (item_category) = 'App:carcontrol' ) 
    THEN 'Unable to fetch door status'


    WHEN 
    (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_climate_wrong_usage_mode' AND TRIM (item_category) = 'App:carcontrol' ) OR 
    (TRIM(content_type) ='error_event' AND TRIM (item_id) = 'cc_carpos_error_wrong_usage_mode' AND TRIM (item_category) = 'App:carcontrol' ) 
    THEN 'Wrong Usage Mode'

    WHEN 
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'airquality_start_purifying_click' AND TRIM (item_category) = 'App:carcontrol:airquality' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_air_quality_start' AND TRIM (item_category) = 'App:carcontrol' ) OR 
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_start' AND TRIM (item_category) = 'App:carcontrol:climate' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_climate_start' AND TRIM (item_category) = 'App:carcontrol' ) 
    THEN 'Start'

    WHEN
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'airquality_stop_purifying_click' AND TRIM (item_category) = 'App:carcontrol:airquality' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_air_quality_stop' AND TRIM (item_category) = 'App:carcontrol' ) OR 
    (TRIM(content_type) ='action_click' AND TRIM (item_id) = 'climate_stop' AND TRIM (item_category) = 'App:carcontrol:climate' ) OR 
    (TRIM(content_type) ='tile_action_click' AND TRIM (item_id) = 'tile_climate_stop' AND TRIM (item_category) = 'App:carcontrol' ) 
    THEN 'Stop'

    
else 'Other'
end
{% endmacro %}