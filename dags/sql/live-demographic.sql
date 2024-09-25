with grouped_data as (
	select 
		client_id 
		, zone_id 
		, age 
		, gender 
		, emotion 
		, count(id) 
	from viseetor_raw vr 
	group by client_id 
		, zone_id 
		, age 
		, gender 
		, emotion
)
, rank_data as (
	select 
		id
		, created_at::timestamptz 
		, recording_time::timestamptz 
		, age 
		, client_id 
		, zone_id 
		, gender 
		, emotion 
		, row_number () over (partition by age, client_id, gender, emotion order by created_at) as rank
	from viseetor_raw vr2 
	--where created_at between '2024-09-17 13:20:00' and '2024-09-17 13:24:59'
)
insert into live_demographic (created_at, client_id, zone_id, count, age, gender, emotion, record_time)
select distinct on(gd.age, gd.gender, gd.emotion, gd.client_id, gd.zone_id)
	rd.created_at
	, gd.client_id
	, gd.zone_id
	, gd.count
	, gd.age
	, gd.gender
	, gd.emotion
	, rd.recording_time as record_time
from grouped_data gd
join rank_data rd 
on gd.client_id = rd.client_id
	and gd.zone_id = rd.zone_id
	and gd.age = rd.age
	and gd.gender = rd.gender
	and gd.emotion = rd.emotion
where rd.rank = 1;