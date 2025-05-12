insert into historical_visitor (created_at, client_id, zone_id, median, gender, record_time, record_time_end, avg_dwell_time, average, mode, max, store_id, detection_type)

select 
	created_at
	, client_id
	, zone_id
	, median
	, gender
	, record_time
	, record_time_end
	, avg_dwell_time
	, average
	, mode
	, max
	, store_id
	, detection_type
from live_visitor
WHERE (record_time AT TIME ZONE 'Asia/Jakarta')::date = '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_date") }}';

DELETE FROM live_visitor WHERE (record_time AT TIME ZONE 'Asia/Jakarta')::date = '{{ ti.xcom_pull(task_ids="get_time_filter", key="filter_date") }}';