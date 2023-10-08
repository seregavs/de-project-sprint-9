-- DROP SCHEMA stg;

CREATE SCHEMA stg AUTHORIZATION db_user;

-- DROP SEQUENCE stg.order_events_id_seq;

CREATE SEQUENCE stg.order_events_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;-- stg.order_events definition

-- Drop table

-- DROP TABLE stg.order_events;

CREATE TABLE stg.order_events (
	id serial4 NOT NULL,
	object_id int4 NOT NULL,
	payload json NOT NULL,
	object_type varchar NOT NULL,
	sent_dttm timestamp NOT NULL,
	CONSTRAINT order_events_pk PRIMARY KEY (id),
	CONSTRAINT order_events_un UNIQUE (object_id)
);
