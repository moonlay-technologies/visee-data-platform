WITH raw_data AS (
SELECT 
    client_id
    , zone_id
    , gender 
    , CAST(age AS INTEGER) AS age
    , emotion
    , avg(duration) as avg_dwell_time
    , count(object_id) as count_object
FROM demographic de
group by
    client_id
    , zone_id
    , gender 
    , age
    , emotion
order by age, gender, emotion 
) 
, final_result as (
select
	current_timestamp ::TIMESTAMPTZ as "date"
	, rd.client_id
	, mc.name as client_name
	, rd.zone_id
	, mz.zone_name as zone_name
    , rd.gender
    , rd.age
    , rd.emotion
 	, rd.count_object as count
    , rd.avg_dwell_time
    , current_timestamp ::TIMESTAMPTZ as created_at 
from raw_data rd
left join master_client mc on rd.client_id = mc.id
left join master_zone mz on rd.zone_id = mz.id
group by
	rd.client_id 
	, mc.name
	, rd.zone_id 
	, mz.zone_name
	, rd.gender
	, rd.count_object
	, rd.age
	, rd.emotion
	, rd.avg_dwell_time
order by rd.age, rd.gender, rd.emotion
) 
insert into history_demographic ("date", created_at ,client_id, client_name, zone_id, zone_name, gender, age, "count", avg_dwell_time, emotion)
select 
	"date"
	, created_at 
	, client_id 
	, client_name
	, zone_id 
	, zone_name
	, gender
	, age
	, "count"
	, avg_dwell_time
	, emotion
from final_result;