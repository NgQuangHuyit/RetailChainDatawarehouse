FROM hadoopbase:test

ARG HIVE_URL=https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz

RUN curl -fL ${HIVE_URL} -o /tmp/apache-hive-3.1.3-bin.tar.gz \
    && tar -xvf /tmp/apache-hive-3.1.3-bin.tar.gz -C /opt \
    && rm -rf /tmp/apache-hive-3.1.3-bin.tar.gz.ta.gz


ENV HIVE_HOME=/opt/apache-hive-3.1.3-bin
ENV PATH=$PATH:$HIVE_HOME/bin
ENV CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/*:$HIVE_HOME/lib/*


COPY /hive-site.xml ${HIVE_HOME}/conf/hive-site.xml


WORKDIR $HIVE_HOME

EXPOSE 10000 10002

CMD ["tail", "-f", "/dev/null"]