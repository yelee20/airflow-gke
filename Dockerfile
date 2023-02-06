FROM --platform=linux/amd64 apache/airflow:2.4.1-python3.8

WORKDIR ${AIRFLOW_HOME}

COPY plugins/ plugins/
COPY requirements.txt .

RUN pip3 install -r requirements.txt