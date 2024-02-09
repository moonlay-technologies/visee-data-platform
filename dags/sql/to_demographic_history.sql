WITH raw_data AS (
    SELECT 
        client_id, 
        zone_id, 
        gender, 
        CAST(age AS INTEGER) AS age, 
        object_id, 
        duration, 
        emotion,
        confidence
    FROM demographic de
    -- WHERE date(created_at) >= :filter_start AND date(created_at) <= :filter_end
) 
, final_result as (
select
	current_timestamp ::TIMESTAMPTZ as "date"
	, client_id
	, 'Moonlay' as client_name
	, zone_id
	, 'Zone_25H' as zone_name
	, count(object_id) as "count"
    , gender
    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) AS age
    , (SELECT emotion FROM raw_data GROUP BY emotion ORDER BY COUNT(*) DESC LIMIT 1) AS emotion
    , avg(duration) as avg_dwell_time
    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY confidence) AS confidence
    , current_timestamp ::TIMESTAMPTZ as  created_at 
from raw_data
group by
	client_id 
	, zone_id 
	, gender
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
	, age::int as age
	, "count"
	, avg_dwell_time
	, emotion
from final_result;