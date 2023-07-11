import json
import logging
import os
from copy import copy
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from parsing_lawsuits.python_callables import (FlatLawsuit, Lawsuit,
                                               LawsuitDocument,
                                               calculate_grades,
                                               get_electronic_cases,
                                               get_lawsuits,
                                               preprocessing_data)

description = """Даг сбора и анализа судебных исков"""


default_args = {
    "owner": "DieNice",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22, 10, 0, 0),
    "email": [""],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG("lawsuits_processing", default_args=default_args,
          schedule_interval=None,
          catchup=False, description=description,
          max_active_runs=1,
          tags=["logs"])

MAILING_LIST_USERS = [""]


def extract_lawsuits_task(**kwargs) -> str:
    """Извлекает судебные иски с сайта
    """
    name_company = kwargs["name_company"]
    deep = kwargs["deep"]
    raw_lawsuits = get_lawsuits(name_company)
    lawsuits = get_electronic_cases(raw_lawsuits, deep)
    digested_data = preprocessing_data(lawsuits)
    return json.dumps([asdict(row) for row in digested_data], default=str)


def preprocessing_task(**kwargs) -> List[Dict[str, Any]]:
    """Обработка судебных исков
    """
    ti = kwargs['ti']
    lawsuits_raw = ti.xcom_pull(task_ids=["extract_lawsuits_task"])[0]
    flat_lawsuits = json.loads(lawsuits_raw)
    flat_lawsuits = [FlatLawsuit(**flat_lawsuit)
                     for flat_lawsuit in flat_lawsuits]
    # flat_lawsuits = preprocessing_data(lawsuits)

    return json.dumps([asdict(flat_lawsuit) for flat_lawsuit in flat_lawsuits], default=str)


def analyze_lawsuit_task(**kwargs) -> str:
    """Подсчитывает судебные оценки
    """
    ti = kwargs['ti']
    AuthC = kwargs["AuthC"]
    AsC = kwargs["AsC"]
    flat_lawsuits_dicts = ti.xcom_pull(task_ids="preprocessing_task")
    flat_lawsuits = json.loads(flat_lawsuits_dicts)
    
    grades = calculate_grades([FlatLawsuit(**flat_dict)
                               for flat_dict in flat_lawsuits],
                              AuthC, AsC)

    return json.dumps(grades, default=str)


def load_to_clickhouse_task(**kwargs) -> None:
    """Загружает оценки критериев в CH
    """
    ti = kwargs['ti']
    grades = ti.xcom_pull(task_ids="analyze_lawsuit_task")


t_1 = PythonOperator(task_id="extract_lawsuits_task",
                     python_callable=extract_lawsuits_task, dag=dag,  op_kwargs={"name_company": "Ростикс", "deep": 5})

t_2 = PythonOperator(task_id="preprocessing_task",
                     python_callable=preprocessing_task, dag=dag)

t_3 = PythonOperator(
    task_id="analyze_lawsuit_task", python_callable=analyze_lawsuit_task, dag=dag,
    op_kwargs={"AuthC": 800_000,
               "AsC": 699_900_000})

t_4 = PythonOperator(task_id="load_to_clickhouse_task",
                     python_callable=load_to_clickhouse_task, dag=dag)

t_1.set_downstream([t_2])
t_2.set_downstream([t_3])
t_3.set_downstream([t_4])
