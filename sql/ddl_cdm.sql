create schema if not exists cdm;

create table if not exists cdm.dm_courier_ledger (
	 id serial
	,courier_id integer
	,courier_name varchar
	,settlement_year integer
	,settlement_month integer
	,orders_count integer
	,orders_total_sum numeric(14, 2)
	,rate_avg numeric
	,order_processing_fee numeric
	,courier_order_sum numeric(14, 2)
	,courier_tips_sum numeric(14, 2)
	,courier_reward_sum numeric(14, 2)
	,constraint dm_courier_ledger_id_pkey primary key (id)
);

--drop table cdm.dm_courier_ledger;
