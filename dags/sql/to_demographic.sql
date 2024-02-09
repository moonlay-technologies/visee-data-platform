with 
raw_data as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, gender
	, cast(age as integer) as age
	, emotion
	, CAST(recording_time AS TIMESTAMP) AS created_at
	, CAST(recording_time AS TIMESTAMP) AS updated_at
	, confidence
from raw_table 
--where 
--	(created_at::TIMESTAMPTZ >=  %(filter_start)s::timestamptz AND 
--	created_at::TIMESTAMPTZ <= %(filter_end)s::timestamptz)
order by object_id, gender, emotion
)
, age_median as (
select 
	client_id
	, device_id
	, session_id
	, zone_id
	, object_id
	, gender
	, emotion
	, percentile_cont(0.5) within group (order by age) as median_age
	, confidence
from raw_data
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, gender
	, emotion
	, confidence
) 
, confidence_max as (
select
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
--	, gender
--	, emotion
	, max(confidence) as max_confidence
from age_median
group by
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
--	, gender
--	, emotion
order by object_id-- , gender , emotion 
) 
, emotion_mode as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
--	, gender
--	, age
--	, count(gender) as gender_mode
	, emotion
	, count(emotion) as mode_emotion
	, row_number() over (partition by object_id order by count(emotion) desc) as rank_emotion
from age_median
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
--	, gender
--	, age
	, emotion
order by object_id
) 
, get_time as ( 
select 
    rw.client_id 
    , rw.device_id 
    , rw.session_id 
    , rw.zone_id 
    , rw.object_id 
    , min(case when v."in" is not null then v."in" else rw.created_at end) as "in"
    , max(rw.created_at) as "out"
from raw_data rw 
left join visitor v on rw.client_id::int = v.client_id::int
    and rw.device_id::int = v.device_id::int
    and rw.session_id::uuid = v.session_id::uuid
    and rw.zone_id::int = v.zone_id::int
    and rw.object_id::int = v.object_id::int
group by 
    rw.client_id 
    , rw.device_id 
    , rw.session_id 
    , rw.zone_id 
    , rw.object_id 
) 
, final_query as (
select 
	current_timestamp as created_at 
	, current_timestamp as updated_at 
	, am.client_id 
	, am.device_id 
	, am.session_id  
	, am.object_id 
	, am.zone_id
	, date(gt."in") as "date"
	, gt."in"
	, gt."out"
	, (gt."out" - gt."in") as duration
	, am.gender 
	, em.emotion
	, round(percentile_cont(0.5) within group (order by am.median_age)) as age
	, cm.max_confidence as confidence
	, em.rank_emotion
from age_median am
join emotion_mode em on em.object_id = am.object_id and em.rank_emotion = 1
join confidence_max cm on cm.object_id = am.object_id
join get_time gt on gt.object_id = am.object_id
group by 
	am.client_id
	, am.device_id
	, am.session_id
	, am.zone_id
	, am.object_id
	, am.gender
	, em.emotion
	, cm.max_confidence
	, gt."in"
	, gt."out"
	, em.rank_emotion
)
INSERT INTO demographic (created_at, updated_at, client_id, device_id, session_id, object_id, zone_id, "date", "in", "out", duration, gender, age, emotion, confidence)
SELECT
    created_at 
    ,updated_at
    ,client_id
    ,device_id
    ,session_id
    ,object_id
    ,zone_id
    ,"date"
	,"in"
	,"out"
    ,duration
    ,gender
    ,age
    ,emotion
    ,confidence
FROM final_query;