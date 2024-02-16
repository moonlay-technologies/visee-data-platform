with 
raw_data as (
select 
	client_id::int
	, device_id::int
	, session_id::uuid
	, zone_id::int
	, object_id::int
	, gender
	, cast(age as integer) as age
	, emotion
	, CAST(recording_time AS TIMESTAMP) AS created_at
	, CAST(recording_time AS TIMESTAMP) AS updated_at
	, cast(confidence as float) as confidence
from raw_table 
where object_id ~ E'^\\d+$'
--where 
--	(created_at::TIMESTAMPTZ >=  %(filter_start)s::timestamptz AND 
--	created_at::TIMESTAMPTZ <= %(filter_end)s::timestamptz)
order by object_id, gender, emotion
) 
--calculating median value of age per object
, age_median as ( 
select 
	client_id
	, device_id
	, session_id
	, zone_id
	, object_id
	, percentile_cont(0.5) within group (order by age) as median_age
from raw_data
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
) 
--calculating maximum value of confidence per object
, confidence_max as (
select
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, max(confidence) as max_confidence
from raw_data
group by
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
) 
--calculating mode value of gender per object
, gender_count_mode as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, gender
	, count(gender) as mode_gender
	, row_number() over (partition by object_id, session_id, client_id, zone_id, device_id order by count(gender) desc) as rank_gender
from raw_data
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, gender 
order by object_id
) 
, gender_elimination as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, case
	    when count(mode_gender) = 1 then 'Correct'
	else 'Incorrect'
	end as flag
from gender_count_mode
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
) 
, gender_correct_flag as ( 
select 
	gbm.client_id 
	, gbm.device_id 
	, gbm.session_id 
	, gbm.zone_id
	, gbm.object_id
	, gbm.gender
	, e1.flag
	, 'Gender by Mode' as get_by
from gender_elimination e1
join gender_count_mode gbm on e1.client_id = gbm.client_id
    and e1.device_id = gbm.device_id
    and e1.session_id = gbm.session_id
    and e1.zone_id = gbm.zone_id
    and e1.object_id = gbm.object_id
where e1.flag = 'Correct'
) 
, gender_incorrect_stdv as (
select 
	rd.client_id
	, rd.device_id
	, rd.session_id
	, rd.zone_id
	, rd.object_id
	, rd.gender
	, stddev(rd.confidence) as stdv_conf
from gender_count_mode gm 
join gender_elimination e1 
on gm.client_id = e1.client_id
    and gm.device_id = e1.device_id
    and gm.session_id = e1.session_id
    and gm.zone_id = e1.zone_id
    and gm.object_id = e1.object_id
join raw_data rd 
on gm.client_id = rd.client_id
    and gm.device_id = rd.device_id
    and gm.session_id = rd.session_id
    and gm.zone_id = rd.zone_id
    and gm.object_id = rd.object_id
    and gm.gender = rd.gender
where e1.flag = 'Incorrect'
group by 
	rd.client_id
	, rd.device_id
	, rd.session_id
	, rd.zone_id
	, rd.object_id
	, rd.gender
) 
, gender_incorrect_final_stdv as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id
	, object_id 
	, min(stdv_conf) as conf_final
from gender_incorrect_stdv
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id
	, object_id 
) 
, gender_stdv as (
select
	row_number () over (partition by gs.client_id, gs.device_id, gs.session_id, gs.zone_id, gs.object_id  order by random()) as rn
	, gs.client_id 
	, gs.device_id 
	, gs.session_id
	, gs.zone_id
	, gs.object_id
	, gs.gender
	, 'Correct' as flag
	, 'Gender by STDV' as get_by
from gender_incorrect_final_stdv fst
join gender_incorrect_stdv gs on fst.client_id = gs.client_id
    and fst.device_id = gs.device_id
    and fst.session_id = gs.session_id
    and fst.zone_id = gs.zone_id
    and fst.object_id = gs.object_id
    and fst.conf_final = gs.stdv_conf
) 
, gender_union as (
select client_id,device_id,session_id,zone_id,object_id,gender,flag,get_by
from gender_correct_flag
union all
select client_id,device_id,session_id,zone_id,object_id,gender,flag,get_by
from gender_stdv where rn = 1
)
--calculating mode value of emotion per object
, emotion_count_mode as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, emotion
	, count(emotion) as mode_emotion
	, dense_rank() over (partition by object_id, session_id, client_id, zone_id, device_id order by count(emotion) desc) as rank_emotion
from raw_data
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, emotion
) 
, emotion_elimination as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, emotion
	, case
	    when rank_emotion = 1 then 'Correct'
	else 'Incorrect'
	end as flag
from emotion_count_mode
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id 
	, object_id 
	, emotion
	, rank_emotion
) 
, emotion_correct_flag as ( 
select 
	gbm.client_id 
	, gbm.device_id 
	, gbm.session_id 
	, gbm.zone_id
	, gbm.object_id
	, gbm.emotion
	, e1.flag
	, 'Emotion by Mode' as get_by
from emotion_elimination e1
join emotion_count_mode gbm on e1.client_id = gbm.client_id
    and e1.device_id = gbm.device_id
    and e1.session_id = gbm.session_id
    and e1.zone_id = gbm.zone_id
    and e1.object_id = gbm.object_id
    and e1.emotion = gbm.emotion
where e1.flag = 'Correct' 
) 
, emotion_correct_stdv as (
select 
	rd.client_id
	, rd.device_id
	, rd.session_id
	, rd.zone_id
	, rd.object_id
	, rd.emotion
	, stddev(rd.confidence) as stdv_conf
from emotion_count_mode gm 
join emotion_elimination e1 
on gm.client_id = e1.client_id
    and gm.device_id = e1.device_id
    and gm.session_id = e1.session_id
    and gm.zone_id = e1.zone_id
    and gm.object_id = e1.object_id
    and gm.emotion = e1.emotion
join raw_data rd 
on gm.client_id = rd.client_id
    and gm.device_id = rd.device_id
    and gm.session_id = rd.session_id
    and gm.zone_id = rd.zone_id
    and gm.object_id = rd.object_id
    and gm.emotion = rd.emotion
where e1.flag = 'Correct'
group by 
	rd.client_id
	, rd.device_id
	, rd.session_id
	, rd.zone_id
	, rd.object_id
	, rd.emotion
) 
, emotion_conf_null as (
select 
	eis.client_id 
	, eis.device_id 
	, eis.session_id 
	, eis.zone_id 
	, eis.object_id 
	, eis.emotion
	, ecf.flag
	, ecf.get_by
from emotion_correct_stdv eis
join emotion_correct_flag ecf on eis.client_id = ecf.client_id
    and eis.device_id = ecf.device_id
    and eis.session_id = ecf.session_id
    and eis.zone_id = ecf.zone_id
    and eis.object_id = ecf.object_id
where eis.stdv_conf is null 
)
, emotion_correct_final_stdv as (
select 
	client_id 
	, device_id 
	, session_id 
	, zone_id
	, object_id 
	, min(stdv_conf) as conf_final
from emotion_correct_stdv
group by 
	client_id 
	, device_id 
	, session_id 
	, zone_id
	, object_id 
) 
, emotion_stdv as (
    SELECT
        row_number() OVER (PARTITION BY gs.client_id, gs.device_id, gs.session_id, gs.zone_id, gs.object_id ORDER BY random()) AS rn,
        gs.client_id,
        gs.device_id,
        gs.session_id,
        gs.zone_id,
        gs.object_id,
        fst.emotion,
        'Correct' AS flag,
        'Gender by STDV' AS get_by
    FROM emotion_correct_stdv fst
    JOIN emotion_correct_final_stdv gs ON fst.client_id = gs.client_id
        AND fst.device_id = gs.device_id
        AND fst.session_id = gs.session_id
        AND fst.zone_id = gs.zone_id
        AND fst.object_id = gs.object_id
        AND fst.stdv_conf = gs.conf_final
) 
, emotion_union as (
select client_id,device_id,session_id,zone_id,object_id,emotion,flag,get_by
from emotion_stdv
union all
select client_id,device_id,session_id,zone_id,object_id,emotion,flag,get_by
from emotion_conf_null
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
	, am.client_id::int
	, am.device_id::int 
	, am.session_id::uuid  
	, am.object_id::int 
	, am.zone_id::int 
	, date(gt."in") as "date"
	, gt."in"
	, gt."out"
	, (gt."out" - gt."in") as duration
	, ug.emotion
	, round(percentile_cont(0.5) within group (order by am.median_age)) as age
	, cm.max_confidence as confidence
	, em.gender
from age_median am
join gender_union em on em.object_id = am.object_id --and em.rank_emotion = 1
join confidence_max cm on cm.object_id = am.object_id
join get_time gt on gt.object_id = am.object_id
join emotion_union ug on ug.object_id = am.object_id
group by 
	am.client_id
	, am.device_id
	, am.session_id
	, am.zone_id
	, am.object_id
	, ug.emotion
	, cm.max_confidence
	, gt."in"
	, gt."out"
	, em.gender
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