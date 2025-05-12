insert into live_visitor (client_id, store_id, zone_id, record_time, record_time_end, median, average, mode, gender, max, detection_type)
SELECT 
    client_id,
    store_id,
    zone_id,
    min(recording_time) as record_time,
    max(recording_time) as record_time_end,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY male_count) AS median,
    ROUND(AVG(male_count)) AS average,
    MODE() WITHIN GROUP (ORDER BY male_count) AS mode,
    'Male' AS gender,
    MAX(male_count) AS max,
    detection_type
FROM viseetor_line vr
GROUP BY 
    vr.client_id, 
    vr.store_id, 
    vr.zone_id, 
    vr.detection_type

UNION ALL

SELECT 
    client_id,
    store_id,
    zone_id,
    min(recording_time) as record_time,
    max(recording_time) as record_time_end,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY female_count) AS median,
    ROUND(AVG(female_count)) AS average,
    MODE() WITHIN GROUP (ORDER BY female_count) AS mode,
    'Female' AS gender,
    MAX(female_count) AS max,
    detection_type
FROM viseetor_line vr
GROUP BY 
    vr.client_id, 
    store_id,
    vr.store_id, 
    vr.zone_id, 
    vr.detection_type

UNION ALL

SELECT 
    client_id,
    store_id,
    zone_id,
    min(recording_time) as record_time,
    max(recording_time) as record_time_end,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY visitor_count) AS median,
    ROUND(AVG(visitor_count)) AS average,
    MODE() WITHIN GROUP (ORDER BY visitor_count) AS mode,
    'All' AS gender,
    MAX(visitor_count) AS max,
    detection_type
FROM viseetor_line vr
GROUP BY 
    vr.client_id, 
    vr.store_id, 
    vr.zone_id, 
    vr.detection_type;