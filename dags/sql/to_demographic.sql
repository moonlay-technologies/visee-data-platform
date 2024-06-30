
with join_emotion as (
    SELECT object_id, string_agg(DISTINCT emotion, ',') as emotion
    FROM (select object_id, emotion, count(id) from raw_table rt group by object_id, emotion having count(id) >= 1 order by object_id) as raw_table
    GROUP BY object_id
), join_attr as (
	SELECT object_id, string_agg(unnest_attr, ',') as "attributes"
	FROM (
		SELECT distinct 
		    object_id,
		    UNNEST(STRING_TO_ARRAY("attributes", ',')) AS unnest_attr
		FROM (SELECT object_id, "attributes", count(id) FROM raw_table rt GROUP BY object_id, "attributes" HAVING count(id) >= 5 ORDER BY object_id) AS filtered_attr
		ORDER BY object_id 
	)  raw_table
	GROUP BY object_id 
), ranked as (
    SELECT count(id) as appear, client_id, device_id, zone_id, session_id, object_id, date(min(recording_time)) as record_date, min(recording_time) as object_in, max(recording_time) as object_out, gender, age, max(confidence) as confidence,
           ROW_NUMBER() OVER (PARTITION BY client_id, device_id, zone_id, session_id, object_id ORDER BY COUNT(*) DESC) AS rn
    FROM raw_table
    GROUP BY client_id, device_id, zone_id, session_id, object_id, gender, age
)

INSERT INTO demographic (created_at, updated_at, client_id, device_id, session_id, object_id, zone_id, "date", "in", "out", duration, gender, age, emotion, attributes, confidence)
SELECT CAST(min(object_in) AS TIMESTAMP) as created_at, 
	CAST(max(object_out) AS TIMESTAMP) as updated_at, 
	client_id::int, 
	device_id::int, 
	session_id::uuid, 
	r.object_id::int , 
	zone_id::int, 
	CAST(min(record_date) AS DATE) as "date", 
	CAST(min(object_in) AS TIMESTAMP) as "in", 
	CAST(max(object_out) AS TIMESTAMP) as "out", 
	EXTRACT(EPOCH FROM (max(object_out)::timestamp - min(object_in)::timestamp)) AS duration,
	gender, 
	age,
	je.emotion as emotion,
	ja.attributes as attributes,
	CAST(max(confidence) as float) as confidence 
FROM (
	SELECT *
	FROM ranked
	WHERE rn = 1 and appear >= 5
) as r
LEFT join join_emotion je on je.object_id = r.object_id
LEFT join join_attr ja on ja.object_id = r.object_id
GROUP BY client_id, device_id, zone_id, session_id, r.object_id, gender, age, je.emotion, ja.attributes