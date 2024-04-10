#!/bin/bash
while true; do 
    hdfs dfs -test -d /spark-logs
    if [ $? -eq 0 ]; then
        echo "Starting spark history server"
        start-history-server.sh
        break
    fi
done

while true; do
    sleep 60
done