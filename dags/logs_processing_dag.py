import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from logs_parser.python_callables import (analyze_logs, extract_random_logs,
                                          slow_parsing_logs, url_taker)

description = """Даг сбора и анализа лог-файлов"""


default_args = {
    "owner": "DieNice",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22, 10, 0, 0),
    "email": [""],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG("logs_processing", default_args=default_args,
          schedule_interval=None,
          catchup=False, description=description,
          max_active_runs=1,
          tags=["logs"])

MAILING_LIST_USERS = [""]


def extract_logs_task(**kwargs) -> str:
    """Извлекает логи сайта
    """
    return extract_random_logs()


def parsing_logs_task(**kwargs)->List[Dict[str,Any]]:
    """Парсинг логов
    """
    ti = kwargs['ti']
    url_file = ti.xcom_pull(task_ids=["extract_logs_task"])[0]
    return slow_parsing_logs(url_file, url_taker)


def analyze_logs_task(**kwargs) -> str:
    """Подсчитывает тональные оценки отзывов
    """
    ti = kwargs['ti']
    logs = ti.xcom_pull(task_ids="parsing_logs_task")
    grades = analyze_logs(logs)
    return json.dumps(grades,default=str)


def load_to_clickhouse_task(**kwargs) -> None:
    """Загружает отзывы с оценками в CH
    """
    ti = kwargs['ti']
    grades = ti.xcom_pull(task_ids="analyze_logs_task")



t_1 = PythonOperator(task_id="extract_logs_task", python_callable=extract_logs_task,dag=dag)

t_2 = PythonOperator(task_id="parsing_logs_task", python_callable=parsing_logs_task,dag=dag)

t_3 = PythonOperator(
        task_id="analyze_logs_task", python_callable=analyze_logs_task,dag=dag)

t_4 = PythonOperator(task_id="load_to_clickhouse_task",python_callable=load_to_clickhouse_task,dag=dag)

t_1.set_downstream([t_2])
t_2.set_downstream([t_3])
t_3.set_downstream([t_4])