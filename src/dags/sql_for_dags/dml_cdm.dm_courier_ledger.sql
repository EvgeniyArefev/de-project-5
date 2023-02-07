with 
prep_dm_courier_ledger as (
	select 
		 fd.courier_id_dwh as courier_id
		,dc.name as courier_name
		,extract(year from fd.order_ts) as settlement_year
		,extract(month from fd.order_ts) as settlement_month
		,fd.order_id_dwh 
		,fd.total_sum 
		,fd.rate 
		,fd.tip_sum 
	from dds.fct_deliveries fd 
	left join dds.dm_couriers dc 
		on fd.courier_id_dwh = dc.courier_id_dwh 
	where order_ts >= date_trunc('month', current_date - interval '2 month') 
), 
prep_dm_courier_ledger_2 as (
	select 
		 courier_id
		,courier_name
		,settlement_year
		,settlement_month
		,count(order_id_dwh) as orders_count
		,sum(total_sum) as orders_total_sum
		,avg(rate) as rate_avg
		,sum(tip_sum) as courier_tips_sum
	from prep_dm_courier_ledger
	group by 
		 courier_id
		,courier_name
		,settlement_year
		,settlement_month
), 
prep_dm_courier_ledger_3 as (
	select 
		 courier_id
		,courier_name
		,settlement_year
		,settlement_month
		,orders_count
		,orders_total_sum
		,rate_avg
		,(orders_total_sum * 0.25) as order_processing_fee
		,case 
			when rate_avg < 4 then greatest(orders_total_sum * 0.05, orders_count * 100)
			when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, orders_count * 150)
			when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, orders_count * 175)
			when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, orders_count * 200)
		 end as courier_order_sum
		,courier_tips_sum
	from prep_dm_courier_ledger_2
)

select 
	 *  
	,(courier_order_sum + courier_tips_sum * 0.95) as courier_reward_sum
from prep_dm_courier_ledger_3


