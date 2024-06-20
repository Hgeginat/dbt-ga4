{% macro error_event_category(content_type) %}

case when item_id in ('cc_not_able_to_start_climate',
	'cc_not_able_to_stop_climate',
	'cc_climate_car_busy',
	'cc_climate_data_sharing_off',
	'cc_climate_wrong_usage_mode',
	'cc_not_able_to_get_climate_status',
	'ct_climate_timer_error_get',
	'cc_climate_already_good',
	'cc_battery_level_low_no_climate',
	'cc_climate_timer_error_get',
	'ct_climate_timer_error_set') then 'Climate'

when item_id in (
	'cc_battery_version_mismatch',
	'cc_not_able_to_get_battery_status',
	'cc_battery_data_sharing_off',
	'cc_battery_all_data_not_available',
	'cc_climate_error_connect_charge_cable') then 'Battery' 

when item_id in (
	'cc_active_entry_version_mismatch',
	'cc_not_able_to_lock',
	'cc_not_able_to_unlock',
	'cc_not_able_to_get_status_of_doors') then 'Lock - Unlock' 

when item_id in (
	'cc_carpos_error_failed_fetch',
	'cc_carpos_error_general',
	'cc_carpos_error_timeout',
	'cc_carpos_error_wrong_usage_mode') then 'Car Position' 

when item_id in (
	'cc_not_able_to_get_precleaning_status',
	'cc_timeout_fetch_precleaning_status'
	) then 'Air Purification' 

when item_id in (
	'cc_loading_timeout',
	'cc_could_not_connect_to_car',
	'cc_login_to_connect',
	'cc_bluetooth_is_deactivated',
	'cc_not_able_to_perform_action_general',
	'cc_not_able_to_init_sdk',
	'cc_not_able_to_get_car_info',
	'cc_unsupported_function_error',
	'cc_bluetooth_now_enabled', 
	'cc_data_sharing_off') then 'General' 
 else '?' END

{% endmacro %}