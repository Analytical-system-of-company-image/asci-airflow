FROM  apache/airflow:2.5.2-python3.10
USER root

RUN apt update
RUN apt update &&\
    apt install build-essential -y
RUN apt-get update; apt-get clean
RUN apt-get install -y wget
RUN apt-get install -y gnupg
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
RUN apt-get update && apt-get -y install google-chrome-stable

USER airflow



COPY .env ./
COPY constraints.txt ./
COPY packages.pth /usr/local/lib/python3.10/site-packages/
RUN export $(cat .env) &&\
    /usr/local/bin/python -m pip install --upgrade pip

RUN pip install apache-airflow-providers-amazon==4.1.0
RUN pip install dostoevsky
RUN python3.10 -m dostoevsky download fasttext-social-network-model
RUN pip install nltk
RUN python3.10 -m nltk.downloader all

RUN pip install scrapingsubsystem
RUN pip install logsparsersubsystem==1.0.6
RUN pip install lawsuitssubsystem==0.1.0

