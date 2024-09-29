CREATE TABLE public.bak_visitor (
	id serial4 NOT NULL,
	created_at timestamptz NULL DEFAULT now(),
	updated_at timestamptz NULL DEFAULT now(),
	client_id int4 NULL,
	device_id int4 NULL,
	session_id uuid NULL,
	object_id int4 NULL,
	zone_id int4 NULL,
	"date" date NULL,
	"in" timestamptz NULL,
	"out" timestamptz NULL,
	duration interval NULL,
	gender varchar NULL,
	age varchar NULL,
	confidence float8 NULL,
	CONSTRAINT pk_bak_visitor PRIMARY KEY (id),
	CONSTRAINT visitor_conflict UNIQUE (id, client_id, device_id, session_id, object_id, zone_id, date)
);


CREATE TABLE public.bak_monitor_state (
	id serial4 NOT NULL,
	created_at timestamp NOT NULL,
	updated_at timestamp NOT NULL,
	client_id int4 NOT NULL,
	client_name varchar NOT NULL,
	zone_id int4 NOT NULL,
	zone_name varchar NOT NULL,
	gender varchar NULL,
	age varchar NULL,
	counts int4 NOT NULL,
	avg_dwelling_time interval NULL,
	CONSTRAINT monitor_conflict UNIQUE (client_id, zone_id, gender, age)
);


CREATE TABLE public.bak_monitor_peak (
	created_at timestamptz NULL,
	updated_at timestamptz NULL,
	client_id int4 NULL,
	client_name varchar NULL,
	zone_id int4 NULL,
	zone_name varchar NULL,
	"hour" varchar NULL,
	gender varchar NULL,
	age varchar NULL,
	count int4 NULL
);