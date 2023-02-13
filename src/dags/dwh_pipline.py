import requests
import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta, datetime

from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator


http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
url = http_conn_id.host

nickname = 'evgeniy-arefev'
cohort = '8'
headers = {
    "X-API-KEY": api_key,
    "X-Nickname": nickname, 
    "X-Cohort": cohort
}

postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'
dwh_hook = PostgresHook(postgres_conn_id)

'''
Settings fot upload layers
'''
def get_setting(setting_table, workflow_key):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM {setting_table}
                    WHERE workflow_key = %(workflow_key)s;
                """,
                {"workflow_key": workflow_key}
            )
            obj = cursor.fetchone()

    connection.close()
    return obj

def save_setting(setting_table, workflow_key, workflow_setting):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    INSERT INTO {setting_table} (workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_setting
                }
            )
    connection.close()

'''
STG Layer
'''
def upload_couriers_api():
    method_url = '/couriers'

    setting_table = 'stg.srv_wf_settings'
    wf_key = 'stg_couriers'
    last_loaded_key = 'last_courier_id'

    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    wf_setting = get_setting(setting_table, wf_key)
    offset = 0 if wf_setting is None else wf_setting[2][last_loaded_key]
    while True:
        params = {'limit': '50', 'sort_field': '_id', 'sort_direction': 'asc', 'offset': f'{offset}'}

        couriers_req = requests.get(url + method_url, headers=headers, params=params)
        couriers_dict = json.loads(couriers_req.content)

        if len(couriers_dict) == 0:
            wf_setting_json = json.dumps({"last_courier_id": str(offset)}, sort_keys=True, ensure_ascii=False)
            
            connection.commit()
            cursor.close()
            connection.close()
            
            save_setting(setting_table, wf_key, wf_setting_json)
            logging.info(f'Writting {offset} rows')
            break

        values = [[value for value in couriers_dict[i].values()] for i in range(len(couriers_dict))]

        sql = f"INSERT INTO stg.couriers (courier_id, name) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(couriers_dict)

def upload_deliveries_api():
    method_url = '/deliveries'

    setting_table = 'stg.srv_wf_settings'
    wf_key = 'stg_deliveries'
    last_loaded_key = 'last_load_date'

    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    wf_setting = get_setting(setting_table, wf_key)

    days_delta = timedelta(days=1)
    start = datetime.combine((date.today() - (7 * days_delta)), datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S') if wf_setting is None else wf_setting[2][last_loaded_key][:-7]
    end = datetime.combine((date.today() - days_delta), datetime.max.time()).strftime('%Y-%m-%d %H:%M:%S')

    offset = 0
    while True:
        params = {
            'limit': '50', 
            'sort_field': 'order_ts', 
            'sort_direction': 'asc', 
            'from': f'{start}', 'to': f'{end}', 
            'offset': f'{offset}'
            }

        deliveries_req = requests.get(url + method_url, headers=headers, params=params)

        if deliveries_req.status_code == 400:
            deliveries_dict = {}
        else:
            deliveries_dict = json.loads(deliveries_req.content)

        if len(deliveries_dict) == 0:
            connection.commit()
            cursor.close()
            connection.close()

            logging.info(f'Writting {offset} rows')
            break

        values = [[value for value in deliveries_dict[i].values()] for i in range(len(deliveries_dict))]

        sql = f"insert into stg.deliveries (order_id, order_ts , delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum) values %s"
        execute_values(cursor, sql, values)

        last_date = values[len(values)-1][1]
        wf_setting_json = json.dumps({"last_load_date": last_date}, sort_keys=True, ensure_ascii=False)
        save_setting(setting_table, wf_key, wf_setting_json)

        offset += len(deliveries_dict)

'''
DDS Layer
'''
#upload dds.dm_orders
def load_raw_orders(last_order_load):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    select order_id, order_ts
                    from stg.deliveries  
                    where order_ts > %(last_order_load)s
                """,
                {
                    "last_order_load": last_order_load
                }
            )
            objs = cursor.fetchall()

    connection.close() 
    return objs

def insert_dds_dm_orders():
    setting_table = 'dds.srv_wf_settings'
    wf_key = 'dds_dm_orders'
    last_loaded_key = 'last_load_date'

    wf_setting = get_setting(setting_table, wf_key)

    days_delta = timedelta(days=1)
    last_order_load = datetime.combine((date.today() - (100 * days_delta)), datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S') if wf_setting is None else wf_setting[2][last_loaded_key] 
    orders_rows = load_raw_orders(last_order_load)

    order_dt_list = []
    values = []
    for row in orders_rows:
        order_id_tpl = (row[0], )
        values.append(order_id_tpl)

        order_dt_list.append(row[1])


    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    sql = f"INSERT INTO dds.dm_orders (order_id_source) VALUES %s"
    execute_values(cursor, sql, values)

    connection.commit()
    cursor.close()
    connection.close()

    if len(order_dt_list) > 0:
        last_load_date = max(order_dt_list)
        wf_setting = json.dumps({"last_load_date": str(last_load_date)})
        save_setting(setting_table, wf_key, wf_setting)

#upload dds.dm_deliveries
def load_raw_deliverys(last_delivery_load):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    select delivery_id, delivery_ts
                    from stg.deliveries  
                    where delivery_ts > %(last_delivery_load)s
                """,
                {
                    "last_delivery_load": last_delivery_load
                }
            )
            objs = cursor.fetchall()

    connection.close() 
    return objs

def insert_dds_dm_deliveries():
    setting_table = 'dds.srv_wf_settings'
    wf_key = 'dds_dm_deliveries'
    last_loaded_key = 'last_load_date'

    wf_setting = get_setting(setting_table, wf_key)

    days_delta = timedelta(days=1)
    last_delivery_load = datetime.combine((date.today() - (100 * days_delta)), datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S') if wf_setting is None else wf_setting[2][last_loaded_key] 
    delivery_rows = load_raw_deliverys(last_delivery_load)

    delivery_dt_list = []
    values = []
    for row in delivery_rows:
        delivery_id_tpl = (row[0], )
        values.append(delivery_id_tpl)

        delivery_dt_list.append(row[1])

    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    sql = f"INSERT INTO dds.dm_deliveries (delivery_id_source) VALUES %s"
    execute_values(cursor, sql, values)

    connection.commit()
    cursor.close()
    connection.close()

    if len(delivery_dt_list) > 0:
        last_load_date = max(delivery_dt_list)
        wf_setting = json.dumps({"last_load_date": str(last_load_date)})
        save_setting(setting_table, wf_key, wf_setting)

#upload dds.dm_couriers
def load_raw_couriers(last_serial_id):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    select id, courier_id, name
                    from stg.couriers 
                    where id > %(last_serial_id)s
                """,
                {
                    "last_serial_id": last_serial_id
                }
            )
            objs = cursor.fetchall()

    connection.close() 
    return objs

def insert_dds_dm_couriers():
    setting_table = 'dds.srv_wf_settings'
    wf_key = 'dds_dm_couriers'
    last_loaded_key = 'last_load_id'

    wf_setting = get_setting(setting_table, wf_key)
    last_serial_id = 0 if wf_setting is None else wf_setting[2][last_loaded_key] 

    couriers_rows = load_raw_couriers(last_serial_id)

    values = []
    couriers_ids_list = []
    for row in couriers_rows:
        values.append(row[1:])
        couriers_ids_list.append(row[0])

    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    sql = f"INSERT INTO dds.dm_couriers (courier_id_source, name) VALUES %s"
    execute_values(cursor, sql, values)

    connection.commit()
    cursor.close()
    connection.close()

    if len(couriers_ids_list) > 0:
        last_load_id = max(couriers_ids_list)
        wf_setting = json.dumps({"last_load_id": str(last_load_id)})
        save_setting(setting_table, wf_key, wf_setting)

#upload dds.fct_deliveries
def create_values_for_fct_deliveries(last_order_date):
    connection = dwh_hook.get_conn()
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                    select 
                        do2.order_id_dwh
                        ,dd.delivery_id_dwh 
                        ,dc.courier_id_dwh 
                        ,d.order_ts
                        ,d.delivery_ts
                        ,d.address
                        ,d.rate
                        ,sum(d.tip_sum) as tip_sum
                        ,sum(d.sum) as total_sum
                    from stg.deliveries d 
                    left join dds.dm_orders do2 
                        on d.order_id = do2.order_id_source 
                    left join dds.dm_deliveries dd 
                        on d.delivery_id = dd.delivery_id_source 
                    left join dds.dm_couriers dc 
                        on d.courier_id  = dc.courier_id_source 
                    where d.order_ts > %(last_order_date)s
                    group by 
                        do2.order_id_dwh
                        ,dd.delivery_id_dwh 
                        ,dc.courier_id_dwh 
                        ,d.order_ts
                        ,d.delivery_ts
                        ,d.address
                        ,d.rate
                """, 
                {
                    "last_order_date": last_order_date
                }
            )
            objs = cursor.fetchall()

    connection.close() 
    return objs

def insert_dds_fct_deliveries():
    setting_table = 'dds.srv_wf_settings'
    wf_key = 'dds_fct_deliveries'
    last_loaded_key = 'last_load_date'

    wf_setting = get_setting(setting_table, wf_key)

    days_delta = timedelta(days=1)
    last_order_load = datetime.combine((date.today() - (100 * days_delta)), datetime.min.time()).strftime('%Y-%m-%d %H:%M:%S') if wf_setting is None else wf_setting[2][last_loaded_key]

    fct_deliveries_rows = create_values_for_fct_deliveries(last_order_load)
    order_dt_list = [row[3] for row in fct_deliveries_rows]


    connection = dwh_hook.get_conn()
    cursor = connection.cursor()

    sql = f"INSERT INTO dds.fct_deliveries (order_id_dwh, delivery_id_dwh, courier_id_dwh, order_ts, delivery_ts, address, rate, tip_sum, total_sum) VALUES %s"
    execute_values(cursor, sql, fct_deliveries_rows)

    connection.commit()
    cursor.close()
    connection.close()

    if len(order_dt_list) > 0:
        last_load_date = max(order_dt_list)
        wf_setting = json.dumps({"last_load_date": str(last_load_date)})
        save_setting(setting_table, wf_key, wf_setting)

def insert_into_cdm_dm_courier_ledger():
    connection = dwh_hook.get_conn()

    with connection:
        with connection.cursor() as cursor:
            sql = open('/lessons/dags/sql_for_dags/dml_cdm.dm_courier_ledger.sql', 'r').read()
            cursor.execute(sql)

            values = cursor.fetchall()

            cursor.execute(
                """
                with dt as (
                    select to_char(date_trunc('month', current_date - interval '1 month'), 'YYYY-MM') as dt_2_mnth_ago
                )

                delete from cdm.dm_courier_ledger
                where replace(concat(settlement_year, '-', to_char(settlement_month, '00')), ' ', '') >= (select dt_2_mnth_ago from dt)
                """
            )

            sql_insert = (
                f"insert into cdm.dm_courier_ledger ("
                f"courier_id, courier_name, settlement_year ,settlement_month,"
                f"orders_count, orders_total_sum, rate_avg, order_processing_fee,"
                f"courier_order_sum, courier_tips_sum, courier_reward_sum"
                f") values %s"
            )

            execute_values(cursor, sql_insert, values)

    connection.close()

with DAG(
    'dwh_pipline',
    schedule_interval = '0 0 * * *',
    catchup=False,
    start_date=datetime.today()
) as dag:
   
    stg_couriers = PythonOperator(
        task_id = "stg_couriers",
        python_callable = upload_couriers_api
    )

    stg_deliveries = PythonOperator(
        task_id = "stg_deliveries",
        python_callable = upload_deliveries_api
    )


    branch_to_dds = DummyOperator(
        task_id = 'branch_to_dds'
    )

    dds_dm_orders = PythonOperator(
        task_id = "dds_dm_orders",
        python_callable = insert_dds_dm_orders
    )

    dds_dm_deliverys = PythonOperator(
        task_id = "dds_dm_delivery",
        python_callable = insert_dds_dm_deliveries
    )

    dds_dm_couriers = PythonOperator(
        task_id = "dds_dm_couriers",
        python_callable = insert_dds_dm_couriers
    )

    dds_fct_deliveries = PythonOperator(
        task_id = "dds_fct_deliveries",
        python_callable = insert_dds_fct_deliveries
    )


    cdm_dm_courier_ledger = PythonOperator(
        task_id = "cdm_dm_courier_ledger",
        python_callable = insert_into_cdm_dm_courier_ledger
    )

    (
        [stg_couriers, stg_deliveries]
        >> branch_to_dds
        >> [dds_dm_orders, dds_dm_deliverys, dds_dm_couriers] >> dds_fct_deliveries
        >> cdm_dm_courier_ledger
    )




