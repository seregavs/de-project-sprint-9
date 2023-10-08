-- DROP SCHEMA dds;

CREATE SCHEMA dds AUTHORIZATION db_user;
-- dds.h_category definition

-- Drop table

-- DROP TABLE dds.h_category;

CREATE TABLE dds.h_category (
	h_category_pk uuid NOT NULL,
	category_name varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_category_pk PRIMARY KEY (h_category_pk)
);


-- dds.h_order definition

-- Drop table

-- DROP TABLE dds.h_order;

CREATE TABLE dds.h_order (
	h_order_pk uuid NOT NULL,
	order_id int4 NOT NULL,
	order_dt timestamp NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_order_pk PRIMARY KEY (h_order_pk)
);


-- dds.h_product definition

-- Drop table

-- DROP TABLE dds.h_product;

CREATE TABLE dds.h_product (
	h_product_pk uuid NOT NULL,
	product_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_product_pk PRIMARY KEY (h_product_pk)
);


-- dds.h_restaurant definition

-- Drop table

-- DROP TABLE dds.h_restaurant;

CREATE TABLE dds.h_restaurant (
	h_restaurant_pk uuid NOT NULL,
	restaurant_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_restaurant_pk PRIMARY KEY (h_restaurant_pk)
);


-- dds.h_user definition

-- Drop table

-- DROP TABLE dds.h_user;

CREATE TABLE dds.h_user (
	h_user_pk uuid NOT NULL,
	user_id varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT h_user_pk PRIMARY KEY (h_user_pk)
);


-- dds.l_order_product definition

-- Drop table

-- DROP TABLE dds.l_order_product;

CREATE TABLE dds.l_order_product (
	hk_order_product_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT hk_order_product_pk PRIMARY KEY (hk_order_product_pk),
	CONSTRAINT l_order_product_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk),
	CONSTRAINT l_order_product_fk_1 FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk)
);


-- dds.l_order_user definition

-- Drop table

-- DROP TABLE dds.l_order_user;

CREATE TABLE dds.l_order_user (
	hk_order_user_pk uuid NOT NULL,
	h_order_pk uuid NOT NULL,
	h_user_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT hk_order_user_pk PRIMARY KEY (hk_order_user_pk),
	CONSTRAINT l_order_user_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk),
	CONSTRAINT l_order_user_fk_1 FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk)
);


-- dds.l_product_category definition

-- Drop table

-- DROP TABLE dds.l_product_category;

CREATE TABLE dds.l_product_category (
	hk_product_category_pk uuid NOT NULL,
	h_category_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT hk_product_category_pk PRIMARY KEY (hk_product_category_pk),
	CONSTRAINT l_product_category_fk FOREIGN KEY (h_category_pk) REFERENCES dds.h_category(h_category_pk),
	CONSTRAINT l_product_category_fk_1 FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk)
);


-- dds.l_product_restaurant definition

-- Drop table

-- DROP TABLE dds.l_product_restaurant;

CREATE TABLE dds.l_product_restaurant (
	hk_product_restaurant_pk uuid NOT NULL,
	h_restaurant_pk uuid NOT NULL,
	h_product_pk uuid NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	CONSTRAINT hk_product_restaurant_pk PRIMARY KEY (hk_product_restaurant_pk),
	CONSTRAINT l_product_restaurant_product_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk),
	CONSTRAINT l_product_restaurant_restaurant_fk FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk)
);


-- dds.s_order_cost definition

-- Drop table

-- DROP TABLE dds.s_order_cost;

CREATE TABLE dds.s_order_cost (
	h_order_pk uuid NOT NULL,
	"cost" numeric(19, 5) NOT NULL DEFAULT 0,
	payment numeric(19, 5) NOT NULL DEFAULT 0,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_cost_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_cost_check CHECK ((cost > (0)::numeric)),
	CONSTRAINT s_order_cost_payment_check CHECK ((payment > (0)::numeric)),
	CONSTRAINT s_order_cost_pk PRIMARY KEY (h_order_pk),
	CONSTRAINT s_order_cost_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk)
);


-- dds.s_order_status definition

-- Drop table

-- DROP TABLE dds.s_order_status;

CREATE TABLE dds.s_order_status (
	h_order_pk uuid NOT NULL,
	status varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_order_status_hashdiff uuid NOT NULL,
	CONSTRAINT s_order_status_pk PRIMARY KEY (h_order_pk),
	CONSTRAINT s_order_status_fk FOREIGN KEY (h_order_pk) REFERENCES dds.h_order(h_order_pk)
);


-- dds.s_product_names definition

-- Drop table

-- DROP TABLE dds.s_product_names;

CREATE TABLE dds.s_product_names (
	h_product_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_product_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_product_names_pk PRIMARY KEY (h_product_pk),
	CONSTRAINT s_product_names_fk FOREIGN KEY (h_product_pk) REFERENCES dds.h_product(h_product_pk)
);


-- dds.s_restaurant_names definition

-- Drop table

-- DROP TABLE dds.s_restaurant_names;

CREATE TABLE dds.s_restaurant_names (
	h_restaurant_pk uuid NOT NULL,
	"name" varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_restaurant_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_restaurant_names_pk PRIMARY KEY (h_restaurant_pk),
	CONSTRAINT s_restaurant_names_fk FOREIGN KEY (h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk)
);


-- dds.s_user_names definition

-- Drop table

-- DROP TABLE dds.s_user_names;

CREATE TABLE dds.s_user_names (
	h_user_pk uuid NOT NULL,
	username varchar NOT NULL,
	userlogin varchar NOT NULL,
	load_dt timestamp NOT NULL,
	load_src varchar NOT NULL,
	hk_user_names_hashdiff uuid NOT NULL,
	CONSTRAINT s_user_names_pk PRIMARY KEY (h_user_pk),
	CONSTRAINT s_user_names_fk FOREIGN KEY (h_user_pk) REFERENCES dds.h_user(h_user_pk)
);

