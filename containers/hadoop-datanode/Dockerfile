FROM hadoopbase:test

RUN mkdir /hadoop-data/dfs/data \
    && echo "hadoop-namenode" >> /etc/hadoop/masters

COPY /run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]