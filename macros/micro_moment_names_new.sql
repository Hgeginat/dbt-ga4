{% macro micro_moment_names_new(micro_id) %}

case
WHEN CAST (item_id AS STRING) ='149425364' THEN 'The Polestar Digital Key'
WHEN CAST (item_id AS STRING) ='148915376' THEN 'Polestar 4. The SUV coupé transformed'
WHEN CAST (item_id AS STRING) ='149158026' THEN 'The Allebike Alpha Polestar edition'
WHEN CAST (item_id AS STRING) ='148850071' THEN 'YouTube in-car app'
WHEN CAST (item_id AS STRING) ='148851138' THEN 'Polestar and Candela. The next phase'
WHEN CAST (item_id AS STRING) ='148916078' THEN 'Polestar Sustainability Report 2022'
WHEN CAST (item_id AS STRING) ='148915927' THEN 'MY24 Polestar 2 loses three tonnes in three years'
WHEN CAST (item_id AS STRING) ='148816617' THEN 'Apple CarPlay update in Polestar 2'
WHEN CAST (item_id AS STRING) ='149212279' THEN 'The force. Quantified.'
WHEN CAST (item_id AS STRING) ='149212072' THEN 'Remote actions: Polestar and Google'
WHEN CAST (item_id AS STRING) ='149211948' THEN 'Smarter charging with Tibber'
WHEN CAST (item_id AS STRING) ='149210757' THEN 'Pre-entry climate control'
WHEN CAST (item_id AS STRING) ='149132712' THEN 'Sitting down is the only signal the Polestar 2 needs to let it know its go time.' 
WHEN CAST (item_id AS STRING) ='148727834' THEN 'oogle Remote Voice Assistant'  
END
 
{% endmacro %}