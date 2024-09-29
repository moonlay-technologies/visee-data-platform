 
WITH ranked_gender AS (
    SELECT 
        client_id,
        zone_id,
        object_id,
        session_id,
        gender,
        count(id) as total,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, zone_id, object_id, session_id
            ORDER BY count(id) DESC
        ) as gender_rank
    FROM viseetor_raw
    GROUP BY 
        client_id, 
        zone_id, 
        object_id, 
        session_id,
        gender
), ranked_emotion AS (
    SELECT 
        client_id,
        zone_id,
        object_id,
        session_id,
        emotion,
        count(id) as total,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, zone_id, object_id, session_id
            ORDER BY count(id) DESC
        ) as emotion_rank
    FROM viseetor_raw
    GROUP BY 
        client_id, 
        zone_id, 
        object_id, 
        session_id,
        emotion
), ranked_age AS (
    SELECT 
        client_id,
        zone_id,
        object_id,
        session_id,
        age,
        count(id) as total,
        ROW_NUMBER() OVER (
            PARTITION BY client_id, zone_id, object_id, session_id
            ORDER BY count(id) DESC
        ) as age_rank
    FROM viseetor_raw
    GROUP BY 
        client_id, 
        zone_id, 
        object_id, 
        session_id,
        age
)


insert into live_demographic (record_time, record_time_end, avg_dwell_time, client_id, zone_id, object_id, session_id, gender, emotion, age)
SELECT 
	min(recording_time) as recording_time,
	max(recording_time) as recording_time_end,
	max(recording_time) - min(recording_time) as dwell_time,
    vr.client_id,
    vr.zone_id,
    vr.object_id,
    vr.session_id::uuid,
    rg.gender,
    re.emotion,
    ra.age
FROM viseetor_raw vr
join ranked_gender	rg on vr.client_id = rg.client_id and vr.zone_id = rg.zone_id and vr.object_id = rg.object_id and vr.session_id = rg.session_id
join ranked_emotion re on vr.client_id = re.client_id and vr.zone_id = re.zone_id and vr.object_id = re.object_id and vr.session_id = re.session_id
join ranked_age ra on vr.client_id = ra.client_id and vr.zone_id = ra.zone_id and vr.object_id = ra.object_id and vr.session_id = ra.session_id
where rg.gender_rank = 1 and re.emotion_rank = 1 and ra.age_rank = 1
GROUP BY 
    vr.client_id, 
    vr.zone_id, 
    vr.object_id, 
    vr.session_id,
    rg.gender,
    re.emotion,
    ra.age