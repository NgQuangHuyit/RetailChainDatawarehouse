#!/bin/bash

namedir=/hadoop-data/dfs/name

if [ ! -d $namedir ]; then
  echo "Namenode name directory not found: $namedir"
  exit 2
fi

echo "remove lost+found from $namedir"
rm -r $namedir/lost+found

if [ "`ls -A $namedir`" == "" ]; then
  echo "Formatting namenode name directory: $namedir"
  $HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR namenode -format
fi

service ssh start

$HADOOP_HOME/sbin/start-all.sh

# Keep the container running
while true; do
  sleep 60
done