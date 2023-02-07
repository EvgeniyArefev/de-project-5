# Проект «DWH для нескольких источников»

### Описание

Целью проекта является построение хранилища данных от источников до витрины.  
Используемые технолгии: `PostgeSQL`, `Airflow`.

### Как работать с репозиторием

1. В папке `src\dags` хранятся файлы для запуска `DAG`

### Структура репозитория

- `/src/dags`

### Запуск контейнера

Запустите локально команду:

```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-sprint-5-server-local \
sindb/de-pg-cr-af:latest
```

После того как запустится контейнер, будут доступны:

- Airflow

  - `localhost:3000/airflow`

  login: AirflowAdmin.
  пароль: airflow_pass.

- БД
  - `jovyan:jovyan@localhost:15432/de`
