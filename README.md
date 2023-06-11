# logs_analyzer_airflow


mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init


1. TODO: Сделать в docker-compose Clickhouse (желательно худой образ)
2. TODO: Отключить примеры дагов
3. TODO: Реализовать даг сбора финансовой отчетности
4. TODO: Реализовать даг сбора судебных исков
5. TODO: Реализовать даг сбора посещаемости из открытых источников