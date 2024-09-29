with get_data as (
    SELECT 
    created_at,client_id,device_id,zone_id,gender,updated_at,session_id,confidence,object_id,id,camera_type,age
    FROM raw_table 
    where camera_type = 'far'
)
INSERT INTO visitor_raw (created_at,client_id,device_id,zone_id,gender,updated_at,session_id,confidence,object_id,id,camera_type,age)
SELECT 
created_at,client_id,device_id,zone_id,gender,updated_at,session_id,confidence,object_id,id,camera_type,age
FROM get_data