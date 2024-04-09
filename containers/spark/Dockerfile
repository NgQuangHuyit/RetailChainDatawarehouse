FROM hadoopbase:test

ARG SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-3.4.2/spark-3.4.2-bin-hadoop3-scala2.13.tgz
RUN curl -fL ${SPARK_DOWNLOAD_URL} -o /tmp/spark \
    && tar -xvf /tmp/spark -C /opt \
    && rm -rf /tmp/spark

ENV SPARK_HOME=/opt/spark-3.4.2-bin-hadoop3-scala2.13
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PATH=$PATH:$SPARK_HOME/sbin
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
RUN echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> ${SPARK_HOME}/conf/spark-env.sh
RUN mkdir $SPARK_HOME/spark-data $SPARK_HOME/spark-apps

WORKDIR $SPARK_HOME

ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8081 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

EXPOSE 8081 7077 6066

RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG


RUN cp $HADOOP_CONF_DIR/core-site.xml $SPARK_HOME/conf  &&\
    cp $HADOOP_CONF_DIR/yarn-site.xml $SPARK_HOME/conf &&\
    cp $HADOOP_CONF_DIR/hdfs-site.xml $SPARK_HOME/conf

COPY ./containers/spark/start-spark.sh /start-spark.sh
COPY ./conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml

COPY ./containers/spark/jars $SPARK_HOME/jars

RUN chmod a+x /start-spark.sh

CMD ["/start-spark.sh"]




