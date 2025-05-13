WITH ranked_rows AS (
    SELECT 
        recording_time,
        recording_end,
        recording_end - recording_time AS dwell_time,
        vr.client_id,
        vr.zone_id,
        vr.object_id,
        vr.session_id::uuid,
        vr.gender,
        vr.emotion,
        vr.age,
        vr.store_id,
        vr.activity,
        vr.attributes,
        vr.detection_type,
        ROW_NUMBER() OVER (
            PARTITION BY vr.client_id, vr.zone_id, vr.object_id, vr.session_id, vr.store_id
            ORDER BY ttl DESC
        ) AS row_num
    FROM viseetor_raw vr
)
INSERT INTO live_demographic (record_time, record_time_end, avg_dwell_time, client_id, zone_id, object_id, session_id, gender, emotion, age, store_id, activity, attributes, detection_type)
SELECT 
    recording_time,
    recording_end,
    dwell_time,
    client_id,
    zone_id,
    object_id,
    session_id,
    gender,
    emotion,
    age,
    store_id,
    activity,
    attributes,
    detection_type
FROM ranked_rows
WHERE row_num = 1
ON CONFLICT (client_id, zone_id, object_id, session_id, store_id) 
DO UPDATE SET 
    record_time_end = EXCLUDED.record_time_end,
    avg_dwell_time = EXCLUDED.avg_dwell_time,
    gender = EXCLUDED.gender,
    emotion = EXCLUDED.emotion,
    age = EXCLUDED.age,
    activity = EXCLUDED.activity,
    attributes = EXCLUDED.attributes;