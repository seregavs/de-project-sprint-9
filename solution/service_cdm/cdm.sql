-- DROP SCHEMA cdm;

CREATE SCHEMA cdm AUTHORIZATION db_user;

-- DROP SEQUENCE cdm.user_category_counters_id_seq;

CREATE SEQUENCE cdm.user_category_counters_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE cdm.user_product_counters_id_seq;

CREATE SEQUENCE cdm.user_product_counters_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- cdm.user_category_counters definition

-- Drop table

-- DROP TABLE cdm.user_category_counters;

CREATE TABLE cdm.user_category_counters (
	id serial4 NOT NULL,
	user_id int8 NOT NULL,
	category_id int8 NOT NULL,
	category_name varchar(100) NOT NULL,
	order_cnt int4 NOT NULL,
	CONSTRAINT user_category_counters_check CHECK ((order_cnt >= 0)),
	CONSTRAINT user_category_counters_pk PRIMARY KEY (id),
	CONSTRAINT user_category_counters_un UNIQUE (user_id, category_id)
);


-- cdm.user_product_counters definition

-- Drop table

-- DROP TABLE cdm.user_product_counters;

CREATE TABLE cdm.user_product_counters (
	id serial4 NOT NULL,
	user_id int8 NOT NULL,
	product_id int8 NOT NULL,
	product_name varchar(100) NOT NULL,
	order_cnt int4 NOT NULL,
	CONSTRAINT user_product_counters_check CHECK ((order_cnt >= 0)),
	CONSTRAINT user_product_counters_pk PRIMARY KEY (id),
	CONSTRAINT user_product_counters_un UNIQUE (user_id, product_id)
);


