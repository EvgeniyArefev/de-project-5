create schema if not exists dds;

--dds.srv_wf_settings
create table if not exists dds.srv_wf_settings (
	 id serial NOT NULL
	,workflow_key varchar NOT NULL
	,workflow_settings json NOT NULL
	,CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id)
	,CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

--dds.dm_orders
create table if not exists dds.dm_orders (
	  order_id_dwh serial primary key
	 ,order_id_source varchar(30) unique not null
);

--dds.dm_deliveries
create table if not exists dds.dm_deliveries (
	 delivery_id_dwh serial primary key
	,delivery_id_source varchar(30) unique
);

--dds.dm_couriers
create table if not exists dds.dm_couriers (
	 courier_id_dwh serial primary key
	,courier_id_source varchar(30) unique
	,name varchar
);

--dds.fct_deliveries
create table if not exists dds.fct_deliveries(
     order_id_dwh integer primary key
	,delivery_id_dwh	integer
	,courier_id_dwh	integer
	,order_ts timestamp
    ,delivery_ts timestamp
	,address varchar
	,rate integer
	,tip_sum numeric (14, 2)
	,total_sum numeric (14, 2)

    ,constraint fct_deliveries_order_id_dwh_fkey
    	foreign key (order_id_dwh)
    	references dds.dm_orders(order_id_dwh)
    	
    ,constraint fct_deliveries_delivery_id_dwh_fkey
	    foreign key (delivery_id_dwh)
	    references dds.dm_deliveries(delivery_id_dwh)

    ,constraint fct_deliveries_courier_id_dwh_fkey
	    foreign key (courier_id_dwh)
	    references dds.dm_couriers(courier_id_dwh)
);

--truncate dds.dm_orders;
--truncate dds.dm_deliveries;
--truncate dds.dm_couriers;
--truncate dds.fct_deliveries;





