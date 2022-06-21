FROM  apache/airflow:2.1.3-python3.8
USER root
RUN apt update &&\
    apt install build-essential -y

USER airflow

COPY .env ./
COPY constraints.txt ./
COPY custom_packages_install.sh ./
COPY packages.pth /usr/local/lib/python3.8/site-packages/

RUN pip install scrapingsubsystem

RUN python3.8 -m nltk.downloader all
RUN python3.8 -m dostoevsky download fasttext-social-network-model


RUN export $(cat .env) &&\
    /usr/local/bin/python -m pip install --upgrade pip && source custom_packages_install.sh