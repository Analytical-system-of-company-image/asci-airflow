import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from scraping_subsystem.airflow_python_callables import (
    calculation_sentiment_marks, extract_reviews, load_to_kafka)
from scraping_subsystem.scraper.spiders.flamp_spider import (
    FlampSpider, GeneratorStartUrlFlampSpider)

description = """Даг скраппинга и сантимент анализа отзывов"""


default_args = {
    "owner": "DieNice",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 22, 10, 0, 0),
    "email": [""],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0
}

dag = DAG("reviews_processing", default_args=default_args,
          schedule_interval="0 14 * * *",
          catchup=False, description=description,
          max_active_runs=1,
          tags=["reviews"])

MAILING_LIST_USERS = [""]
KAFKA_HOST = os.getenv("KAFKA_HOST")
NUM_CRAWLERS = 1
NAMES_SITES = ["flamp.ru"]
START_URLS_CONFIGS = [
    {'generator_start_urls': GeneratorStartUrlFlampSpider,
     'uri': 'kfc_set_restoranov_bystrogo_obsluzhivaniya'}
]

CRAWLERS_CONFIGS = [
    {'spider': FlampSpider,
     'crawl_settings': {}}
]


def generate_start_urls_task(**kwargs) -> str:
    ti = kwargs['ti']

    try:
        Generator = kwargs['generator_start_urls']
        uri = kwargs['uri']
    except KeyError as _:
        return json.dumps([])
    generator = Generator(uri)
    start_urls = generator. get_reviews_start_url()
    return json.dumps(start_urls)


def extract_reviews_task(**kwargs) -> str:
    """Извлекает отызывы с сайта со страницы start_urls
    """
    ti = kwargs['ti']
    id_generate_start_urls_task = kwargs['id_generate_start_urls']
    start_urls = ti.xcom_pull(
        task_ids=[id_generate_start_urls_task])
    start_urls = json.loads(start_urls[0])

    extract_settings = kwargs['extract_settings']
    crawl_settings = {'start_urls': start_urls[:1000],
                      **extract_settings['crawl_settings']}

    reviews = extract_reviews(extract_settings['spider'],
                              crawl_settings=crawl_settings)
    return reviews


def calculation_sentiment_marks_task(**kwargs) -> str:
    """Подсчитывает тональные оценки отзывов
    """
    ti = kwargs['ti']
    id_extract_reviews_task = kwargs['id_extract_reviews']
    reviews = ti.xcom_pull(task_ids=[id_extract_reviews_task])[0]
    valued_reviews = calculation_sentiment_marks(reviews)
    return valued_reviews


def load_to_kafka_task(**kwargs) -> None:
    """Загружает отзывы с оценками в кафку
    """
    ti = kwargs['ti']
    id_culculation = kwargs['id_culculation']
    reviews = ti.xcom_pull(task_ids=[id_culculation])[0]
    load_to_kafka(reviews, kwargs['key'], kwargs['kafka_host'])


join_tasks = DummyOperator(task_id='join_task', dag=dag)

begin_task = DummyOperator(task_id='begin_task', dag=dag)

for i in range(0, NUM_CRAWLERS):
    buf_id_generate_start_urls = f'{NAMES_SITES[i]}_generate_start_urls'
    buf_id_extract = f'{NAMES_SITES[i]}_extract_reviews'
    buf_id_culculation = f'{NAMES_SITES[i]}_calculation_sentiment_marks'
    buf_id_load_to_kafka = f'{NAMES_SITES[i]}_load_to_kafka_task'

    buf_generate_start_urls = PythonOperator(
        task_id=buf_id_generate_start_urls, python_callable=generate_start_urls_task,
        dag=dag, op_kwargs=START_URLS_CONFIGS[i])

    buf_extract_task_kwargs = {'id_generate_start_urls': buf_id_generate_start_urls,
                               'extract_settings': CRAWLERS_CONFIGS[i]}
    buf_extract_task = PythonOperator(
        task_id=buf_id_extract, python_callable=extract_reviews_task,
        dag=dag, op_kwargs=buf_extract_task_kwargs)

    buf_calculation_sentiment_marks_task = PythonOperator(
        task_id=buf_id_culculation, python_callable=calculation_sentiment_marks_task,
        dag=dag, op_kwargs={'id_extract_reviews': buf_id_extract}
    )

    buf_load_to_kafka_task = PythonOperator(task_id=buf_id_load_to_kafka,
                                            python_callable=load_to_kafka_task,
                                            dag=dag,
                                            op_kwargs={'id_culculation': buf_id_culculation,
                                                       'key': NAMES_SITES[i],
                                                       'kafka_host': 'host.docker.internal:9093'})

    begin_task.set_downstream([buf_generate_start_urls])
    buf_generate_start_urls.set_downstream([buf_extract_task])
    buf_extract_task.set_downstream([buf_calculation_sentiment_marks_task])
    buf_calculation_sentiment_marks_task.set_downstream(
        [buf_load_to_kafka_task])
    buf_load_to_kafka_task.set_downstream([join_tasks])
