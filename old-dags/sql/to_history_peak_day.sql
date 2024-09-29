with raw_data AS (
SELECT 
 client_id
 , client_name
 , zone_id
 , zone_name
 , CONCAT(SUBSTRING("hour" FROM 1 FOR 2),':00 - ', (CAST(SUBSTRING("hour" FROM 1 FOR 2) AS INTEGER)+ 1)%24, ':00') as "hour"
 , gender
 , "count"
 , "age"
FROM monitor_peak
WHERE created_at::date = '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}' --use this params because can't use %()s, the "hour"'s querying using %24 and make error when calling parameters
)
, final_result AS (
SELECT 
 client_id
 , client_name
 , zone_id
 , zone_name
 , "hour"
 , gender 
 , MAX("count") as "count"
 , "age"
 , '{{ ti.xcom_pull(task_ids="task_get_filter", key="filter_date") }}'::date as "date"
 , CURRENT_TIMESTAMP as created_at
FROM raw_data  
GROUP BY 
"hour"
, client_id
, client_name
, zone_id
, zone_name
, gender
, "age"
)
INSERT INTO history_peak_day (client_id, client_name, zone_id, zone_name, "hour", gender, "age", "count", "date", created_at)
SELECT 
 client_id
 , client_name
 , zone_id
 , zone_name
 , "hour"
 , gender 
 , "age"
 , "count"
 , "date"
 , created_at
FROM final_result