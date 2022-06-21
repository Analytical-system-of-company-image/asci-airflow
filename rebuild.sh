# Restart, reinstall dependencies working env and rebuild Docker-container airflow-images, starting docker-compose airflow
rm -rf .venv
python3.8 -m venv .venv
. ./.venv/bin/activate
source .env
. ./custom_packages_install.sh
pip install -r requirements-dev.txt
docker-compose -f docker-compose.yaml down
docker-compose build --no-cache
docker-compose -f docker-compose.yaml up -d