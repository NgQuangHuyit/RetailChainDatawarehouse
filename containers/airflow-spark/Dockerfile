FROM apache/airflow:2.9.0

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ARG SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3-scala2.13.tgz
RUN curl -fL ${SPARK_DOWNLOAD_URL} -o /tmp/spark \
    && tar -xvf /tmp/spark -C /opt \
    && rm -rf /tmp/spark

ENV SPARK_HOME=/opt/spark-3.4.2-bin-hadoop3-scala2.13
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf



COPY ./conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
COPY ./conf/core-site.xml $SPARK_HOME/conf/core-site.xml
COPY ./conf/hdfs-site.xml $SPARK_HOME/conf/hdfs-site.xml
COPY ./conf/mapred-site.xml $SPARK_HOME/conf/mapred-site.xml
COPY ./conf/yarn-site.xml $SPARK_HOME/conf/yarn-site.xml


COPY ./containers/airflow-spark/jars $SPARK_HOME/jars

RUN chmod 777 -R $SPARK_HOME

USER airflow

COPY ./containers/airflow-spark/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow

CMD ["airflow", "standalone"]
