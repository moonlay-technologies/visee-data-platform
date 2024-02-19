with
raw_data as (
select
    client_id::int
    , device_id::int
    , session_id::uuid
    , zone_id::int
    , object_id::int
    , case
        when gender ='Man' then 'male'
        else 'female'
    end as gender
    , cast(age as integer) as age
    , emotion
    , CAST(recording_time AS TIMESTAMP) AS created_at
    , CAST(recording_time AS TIMESTAMP) AS updated_at
    , cast(confidence as float) as confidence
from raw_table
where object_id ~ E'^\\d+$'
--and object_id = '2'
--and session_id = 'd1e661cd-44c4-420e-a707-79724dd696b8'
--where
--  (created_at::TIMESTAMPTZ >=  %(filter_start)s::timestamptz AND
--  created_at::TIMESTAMPTZ <= %(filter_end)s::timestamptz)
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
, emotion_count_mode as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion
    , count(emotion) as mode_emotion
from raw_data
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion
), get_best_mode_emotion as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion  
    , dense_rank() over (partition by object_id, session_id, client_id, zone_id, device_id order by mode_emotion desc) as rank_emotion
    , mode_emotion
from emotion_count_mode
where mode_emotion > 1
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion
    , mode_emotion
)
, flaging_emotion as (
select
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , case
        when sum(
            case
                when rank_emotion = 1 then 1 else 0
            end
            ) = 1 then 'Correct'
        else 'Inccorect'
    end as flaging
from get_best_mode_emotion
group by
    client_id
    , device_id
    , session_id
    , zone_id
    , object_id
)
, emotion_by_mode as (
select
    gbme.client_id
    , gbme.device_id
    , gbme.session_id
    , gbme.zone_id
    , gbme.object_id
    , gbme.emotion
    , 'Emotion by Mode' as get_by
from get_best_mode_emotion gbme
join flaging_emotion fe on gbme.client_id = fe.client_id
    and gbme.device_id = fe.device_id
    and gbme.session_id = fe.session_id
    and gbme.zone_id  = fe.zone_id
    and gbme.object_id  = fe.object_id
where
fe.flaging = 'Correct'
and gbme.rank_emotion = 1
)
, emotion_get_stdv as (
select
    rd.client_id
    , rd.device_id
    , rd.session_id
    , rd.zone_id
    , rd.object_id
    , rd.emotion
    , stddev(rd.confidence) as stdv_conf
from get_best_mode_emotion gbme
join raw_data rd on gbme.client_id = rd.client_id
    and gbme.device_id = rd.device_id
    and gbme.session_id = rd.session_id
    and gbme.zone_id  = rd.zone_id
    and gbme.object_id  = rd.object_id
--  and gbme.emotion = rd.emotion
join flaging_emotion fe on gbme.client_id = fe.client_id
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
    , rd.emotion
)
, emotion_final_stdv as (
select
    egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
    , min(egs.stdv_conf) as final_stdv
from emotion_get_stdv egs
group by
    egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
)
, emotion_by_stdv as (
select
    row_number () over(partition by egs.client_id, egs.device_id, egs.session_id, egs.zone_id, egs.object_id order by random()) as rn
    , egs.client_id
    , egs.device_id
    , egs.session_id
    , egs.zone_id
    , egs.object_id
    , egs.emotion
    , 'Emotion by Stdv' as get_by
from emotion_final_stdv efs
join emotion_get_stdv egs on efs.client_id = egs.client_id
    and efs.device_id = egs.device_id
    and efs.session_id = egs.session_id
    and efs.zone_id = egs.zone_id
    and efs.object_id = egs.object_id
    and efs.final_stdv = egs.stdv_conf
)
, union_emotion_1 as (
select client_id,device_id,session_id,zone_id,object_id,emotion,get_by
from emotion_by_mode
union all
select client_id,device_id,session_id,zone_id,object_id,emotion,get_by
from emotion_by_stdv where rn = 1
)
, get_single_emotion_record as (
select
    row_number () over(partition by client_id, device_id, session_id, zone_id, object_id order by random()) as rn
    , client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion
    , 'Emotion by Single' as get_by
    , max(confidence) as conf
from raw_data
where (client_id,device_id,session_id,zone_id,object_id) not in (select client_id,device_id,session_id,zone_id,object_id from union_emotion_1 )
group by
     client_id
    , device_id
    , session_id
    , zone_id
    , object_id
    , emotion
)
, union_emotion_all as (
select client_id,device_id,session_id,zone_id,object_id,emotion,get_by from union_emotion_1
union all
select client_id,device_id,session_id,zone_id,object_id,emotion,get_by from get_single_emotion_record where rn=1
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
, final_result as (
select
    current_timestamp::timestamptz as created_at
    , current_timestamp::timestamptz as updated_at
    , ag.client_id
    , ag.device_id
    , ag.session_id
    , ag.object_id
    , ag.zone_id
    , date("in") as "date"
    , gt."in"
    , gt."out"
    , (gt."out" - gt."in") as duration
    , uga.gender
    , round(percentile_cont(0.5) within group (order by ag.median_age)) as "age"
    , uea.emotion
    , cm.max_confidence as confidence
from age_median ag
join get_time gt on ag.client_id = gt.client_id
    and ag.device_id = gt.device_id
    and ag.session_id = gt.session_id
    and ag.zone_id = gt.zone_id
    and ag.object_id = gt.object_id
join union_emotion_all uea on ag.client_id = uea.client_id
    and ag.device_id = uea.device_id
    and ag.session_id = uea.session_id
    and ag.zone_id = uea.zone_id
    and ag.object_id = uea.object_id
join union_gender_all uga on ag.client_id = uga.client_id
    and ag.device_id = uga.device_id
    and ag.session_id = uga.session_id
    and ag.zone_id = uga.zone_id
    and ag.object_id = uga.object_id
join confidence_max cm on ag.client_id = cm.client_id
    and ag.device_id = cm.device_id
    and ag.session_id = cm.session_id
    and ag.zone_id = cm.zone_id
    and ag.object_id = cm.object_id
group by
    ag.client_id
    , ag.device_id
    , ag.session_id
    , ag.object_id
    , ag.zone_id
    , gt."in"
    , gt."out"
    , uga.gender
    , uea.emotion
    , cm.max_confidence
)
INSERT INTO demographic (created_at, updated_at, client_id, device_id, session_id, object_id, zone_id, "date", "in", "out", duration, gender, age, emotion, confidence)
select
    created_at
    , updated_at
    , client_id
    , device_id
    , session_id    
    , object_id
    , zone_id   
    , "date"    
    , "in"  
    , "out"
    , duration  
    , gender    
    , age   
    , emotion   
    , confidence
from final_result