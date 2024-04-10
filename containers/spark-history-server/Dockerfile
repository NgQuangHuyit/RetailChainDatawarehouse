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


EXPOSE 18080


COPY ./conf/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY ./conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml


COPY ./containers/spark-history-server/jars $SPARK_HOME/jars
COPY ./containers/spark-history-server/start-spark-history-server.sh /start-spark-history-server.sh

RUN chmod a+x /start-spark-history-server.sh

CMD ["/start-spark-history-server.sh"]






