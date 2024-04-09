FROM apache/airflow:slim-2.9.0-python3.8

COPY /requirements.txt /requirements.txt

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update && apt-get install -y \
    wget

USER airflow

CMD ["airflow", "standalone"]
