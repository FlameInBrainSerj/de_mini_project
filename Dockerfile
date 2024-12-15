FROM apache/airflow:latest as development

COPY requirements.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /main

WORKDIR /main
