# Документация

## DDL файлы

- все необходимые `DDL` файлы находятся в папке `sql`
- перед запуском DAG необходимо прогрузить `DDL` файлы

## Airflow

- etl процесс реализован одним DAGом поставленным на расписание `src\dags\dwh_pipline.py`
- Задачи внутри DAG разибиты на TaskGroup

## Структура DWH

#### DWH состоит из следующих слоев данных:

- STG (Staging)

Содержит данные из источников полученных по api. В слое есть системная таблица с контрольными точками загрузок `stg.srv_wf_settings`

- DDS (Detail Data Store)

Данные загружаются последовательно после слоя STG.
В слое есть системная таблица с контрольными точками загрузок `dds.srv_wf_settings`

- CDM (Common Data Marts)

Здесь хранится итоговая витрина для расчётов с курьерами.  
Механизм загрузки: Берем данные по заказам из DDS за последние 2 мес, делаем пересчет данных, удаляем записи за этот срок в витрине и загружаем заново.