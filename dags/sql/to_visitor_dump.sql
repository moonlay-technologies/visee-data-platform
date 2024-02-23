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
   and camera_type = 'far'
   and 
    (created_at::TIMESTAMPTZ >=  %(filter_start)s::timestamptz
    AND 
    created_at::TIMESTAMPTZ <= %(filter_end)s::timestamptz)
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
, gender_count_mode as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender
    , count(gender) as mode_gender
from raw_data
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender
), get_best_mode_gender as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender  
    , dense_rank() over (partition by object_id, session_id, client_id, zone_id, device_id order by mode_gender desc) as rank_gender
    , mode_gender
from gender_count_mode
where mode_gender > 1
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender
    , mode_gender
)
, flaging_gender as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , case
        when sum(
            case
                when rank_gender = 1 then 1 else 0
            end
            ) = 1 then 'Correct'
        else 'Inccorect'
    end as flaging
from get_best_mode_gender
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
)
, gender_by_mode as (
select
    gbme.client_id
    , gbme.device_id
    , gbme.session_id
    , gbme.zone_id
    , gbme.object_id
    , gbme.gender
    , 'Emotion by Mode' as get_by
from get_best_mode_gender gbme
join flaging_gender fe on gbme.client_id = fe.client_id
    and gbme.device_id = fe.device_id
    and gbme.session_id = fe.session_id
    and gbme.zone_id  = fe.zone_id
    and gbme.object_id  = fe.object_id
where
fe.flaging = 'Correct'
and gbme.rank_gender = 1
)
, gender_get_stdv as (
select
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender
    , stddev(rd.confidence) as stdv_conf
from get_best_mode_gender gbme
join raw_data rd on gbme.client_id = rd.client_id
    and gbme.device_id = rd.device_id
    and gbme.session_id = rd.session_id
    and gbme.zone_id  = rd.zone_id
    and gbme.object_id  = rd.object_id
--  and gbme.gender = rd.gender
join flaging_gender fe on gbme.client_id = fe.client_id
    and gbme.device_id = fe.device_id
    and gbme.session_id = fe.session_id
    and gbme.zone_id  = fe.zone_id
    and gbme.object_id  = fe.object_id
where
fe.flaging = 'Inccorect'
group by
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.gender
)
, gender_final_stdv as (
select
    egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
    , min(egs.stdv_conf) as final_stdv
from gender_get_stdv egs
group by
    egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
)
, gender_by_stdv as (
select
    row_number () over(partition by egs.client_id, egs.device_id, egs.session_id, egs.zone_id, egs.object_id order by random()) as rn
    , egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
    , egs.gender
    , 'Gender by Stdv' as get_by
from gender_final_stdv efs
join gender_get_stdv egs on efs.client_id = egs.client_id
    and efs.device_id = egs.device_id
    and efs.session_id = egs.session_id
    and efs.zone_id = egs.zone_id
    and efs.object_id = egs.object_id
    and efs.final_stdv = egs.stdv_conf
)
, union_gender_1 as (
select client_id,device_id,session_id,zone_id,object_id,gender,get_by
from gender_by_mode
union all
select client_id,device_id,session_id,zone_id,object_id,gender,get_by
from gender_by_stdv where rn = 1
)
, get_single_gender_record as (
select
    row_number () over(partition by client_id, device_id, session_id, zone_id, object_id order by random()) as rn
    , client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender
    , 'Gender by Single' as get_by
    , max(confidence) as conf
from raw_data
where (client_id,device_id,session_id,zone_id,object_id) not in (select client_id,device_id,session_id,zone_id,object_id from union_gender_1 )
group by
     client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , gender
)
, union_gender_all as (
select client_id,device_id,session_id,zone_id,object_id,gender,get_by from union_gender_1
union all
select client_id,device_id,session_id,zone_id,object_id,gender,get_by from get_single_gender_record where rn=1
)
, get_time as (
select
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , min(rd.created_at) as "in"
    , max(rd.created_at) as "out"
from raw_data rd
group by
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
)
, final_result AS (
select
    current_timestamp::timestamptz as created_at
    , current_timestamp::timestamptz as updated_at
    , uga.client_id
    , uga.device_id
    , uga.session_id
    , uga.object_id
    , uga.zone_id
    , date(gt."in") as "date"
    , gt."in"
    , gt."out"
    , (gt."out" - gt."in") as duration
	, TO_CHAR(
    	("out" AT TIME ZONE 'Asia/Bangkok') - 
    	MOD(EXTRACT(MINUTE FROM "out" AT TIME ZONE 'Asia/Bangkok'), 5) * INTERVAL '1 minute','HH24:MI') || 
        ' - ' || 
       TO_CHAR(
    	("out" AT TIME ZONE 'Asia/Bangkok') + 
        ((5 - MOD(EXTRACT(MINUTE FROM "out" AT TIME ZONE 'Asia/Bangkok'), 5)) * INTERVAL '1 minute'),'HH24:MI') AS "hour"
	, gender
	, '' as age
	, cm.max_confidence as confidence
from union_gender_all uga
join get_time gt on uga.client_id = gt.client_id
    and uga.device_id = gt.device_id
    and uga.session_id = gt.session_id
    and uga.zone_id = gt.zone_id
    and uga.object_id = gt.object_id
join confidence_max cm on uga.client_id = cm.client_id
    and uga.device_id = cm.device_id
    and uga.session_id = cm.session_id
    and uga.zone_id = cm.zone_id
    and uga.object_id = cm.object_id
)
INSERT INTO visitor_dump (created_at, updated_at, client_id, device_id, session_id, object_id, zone_id, "date", "in", "out", duration,"hour",gender, age, confidence)
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
	, "hour"
	,gender
	,age
	,confidence
FROM final_result
ON CONFLICT on constraint visitor_dump_unq --(client_id, device_id, session_id, object_id, zone_id, "date")
DO nothing 