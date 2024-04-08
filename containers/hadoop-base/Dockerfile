FROM ubuntu:20.04

RUN  apt-get update && apt-get install -y \
    openjdk-8-jdk \
    openssh-server \
    openssh-client \
    curl

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

ARG hadoop_version=3.3.5

ARG HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-${hadoop_version}/hadoop-${hadoop_version}.tar.gz

RUN curl -fL ${HADOOP_URL} -o /tmp/hadoop-${hadoop_version}.tar.gz \
    && tar -xvf /tmp/hadoop-${hadoop_version}.tar.gz -C /opt \
    && rm -rf /tmp/hadoop-${hadoop_version}.ta.gz

RUN ln -s /opt/hadoop-${hadoop_version}/etc/hadoop /etc/hadoop  \
    && mkdir /opt/hadoop-${hadoop_version}/logs \
    && mkdir /hadoop-data \
    && mkdir /hadoop-data/dfs

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys


ENV HADOOP_HOME=/opt/hadoop-${hadoop_version}
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV PATH=$HADOOP_HOME/sbin:$PATH
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/*:.


RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/hadoop/hadoop-env.sh
RUN echo "export HADOOP_SSH_OPTS=\"-o StrictHostKeyChecking=no\"" >> /etc/hadoop/hadoop-env.sh

USER root

COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY conf/mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY conf/yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml

