insert into live_visitor (client_id, zone_id, record_time, record_time_end, median, average, mode, gender)
SELECT 
    client_id,
    zone_id,
    min(recording_time) as record_time,
    max(recording_time) as record_time_end,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY male_peak) AS median,
    ROUND(AVG(male_peak)) AS average,
    MODE() WITHIN GROUP (ORDER BY male_peak) AS mode,
    'Male' AS gender
FROM viseetor_line vr
GROUP BY 
    vr.client_id, 
    vr.zone_id

UNION ALL

SELECT 
    client_id,
    zone_id,
    min(recording_time) as record_time,
    max(recording_time) as record_time_end,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY female_peak) AS median,
    ROUND(AVG(female_peak)) AS average,
    MODE() WITHIN GROUP (ORDER BY female_peak) AS mode,
    'Female' AS gender
FROM viseetor_line vr
GROUP BY 
    vr.client_id, 
    vr.zone_id;