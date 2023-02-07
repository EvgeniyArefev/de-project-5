create schema if not exists stg;

create table if not exists stg.couriers(
	 id serial primary key
	,courier_id varchar(30)
	,name varchar 
);

create table if not exists stg.deliveries(
	 id serial primary key
	,order_id varchar(30)
	,order_ts timestamp
	,delivery_id varchar(30)
	,courier_id varchar(30)
	,address varchar
	,delivery_ts timestamp
	,rate integer
	,sum numeric (14, 2)
	,tip_sum numeric (14, 2)
);