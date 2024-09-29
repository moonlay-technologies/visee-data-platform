insert into historical_demographic (created_at, updated_at, client_id, zone_id, age, gender, emotion, object_id, session_id, record_time_end, record_time, avg_dwell_time)
select 
	gd.created_at
	, gd.updated_at
	, gd.client_id
	, gd.zone_id
	, gd.age
	, gd.gender
	, gd.emotion
	, gd.object_id
	, gd.session_id
	, gd.record_time_end
	, gd.record_time
	, gd.avg_dwell_time
from live_demographic gd
WHERE (record_time AT TIME ZONE 'Asia/Jakarta')::date = '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_date") }}';

DELETE FROM live_demographic WHERE (record_time AT TIME ZONE 'Asia/Jakarta')::date = '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_date") }}';