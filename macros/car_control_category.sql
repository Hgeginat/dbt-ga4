{% macro car_control_category(content_type) %}

case when TRIM(content_type) in (
'charging_timer_save',
'set_amp_limit',
'battery_details'
) then 'Battery'

when TRIM(content_type) in (
'save_timer',
'edit_timer',
'delete_timer',
'start_climate',
'add_timer_click'
) then 'Climate' 

when TRIM(content_type) in (
'lock_unlock'
) then  'Lock - Unlock' 

when TRIM(content_type) in (
'car_position_click',
'car_position_external_map_clicked'
) then 'Car Position' 

when TRIM(content_type) in (
'precleaning_page_view',
'precleaning_click',
'air_quality_click'
) then 'Air purification' 

else '(Other)'
END
 
{% endmacro %}