FROM hadoopbase:test

RUN mkdir /hadoop-data/dfs/data \
    && echo "hadoop-namenode" >> /etc/hadoop/masters

CMD ["tail", "-f", "/dev/null"]