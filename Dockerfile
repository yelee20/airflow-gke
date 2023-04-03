FROM --platform=linux/amd64 apache/airflow:2.4.1-python3.8

USER root
LABEL maintainer="yewon"

RUN apt-get update \
 && apt-get install gcc -y \
 && apt-get install -y --no-install-recommends \
    ca-certificates curl firefox-esr           \
 && rm -fr /var/lib/apt/lists/*                \
 && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz | tar xz -C /usr/local/bin \
 && apt-get purge -y ca-certificates curl


USER airflow

WORKDIR ${AIRFLOW_HOME}

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --user -r requirements.txt

ENV AIRFLOW__SCHEDULER__JOB_HEARTBEAT_SEC=30
ENV AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800
ENV AIRFLOW__SCHEDULER__SCHEDULE_AFTER_TASK_EXECUTION=False
