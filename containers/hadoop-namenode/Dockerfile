FROM hadoopbase:test

RUN mkdir /hadoop-data/dfs/name \
    && echo "hadoop-namenode" >> /etc/hadoop/masters \
    && service ssh start

COPY /workers /etc/hadoop/workers

CMD ["tail", "-f", "/dev/null"]