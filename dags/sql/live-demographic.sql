INSERT INTO live_demographic (record_time, record_time_end, avg_dwell_time, client_id, zone_id, object_id, session_id, gender, emotion, age, store_id, activity, attributes, detection_type)
SELECT 
    max(recording_time) AS recording_time,
    max(recording_end) AS recording_time_end,
    max(recording_end) - max(recording_time) AS dwell_time,
    vr.client_id,
    vr.zone_id,
    vr.object_id,
    vr.session_id::uuid,
    vr.gender,
    vr.emotion,
    vr.age,
    vr.store_id,
    max(vr.activity) AS activity,
    max(vr.attributes) AS attributes,
    vr.detection_type
FROM viseetor_raw vr
GROUP BY 
    vr.client_id, 
    vr.zone_id, 
    vr.object_id, 
    vr.session_id,
    vr.gender,
    vr.emotion,
    vr.age,
    vr.store_id,
    vr.detection_type
ON CONFLICT (client_id, zone_id, object_id, session_id, store_id) 
DO UPDATE SET 
    record_time_end = EXCLUDED.record_time_end,
    avg_dwell_time = EXCLUDED.avg_dwell_time;