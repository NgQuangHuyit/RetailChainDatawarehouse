#!/bin/bash

. "$SPARK_HOME/bin/load-spark-env.sh"

if [ "$SPARK_WORKLOAD" == "master" ];
then

export SPARK_MASTER_HOST=`hostname`

cd $SPARK_HOME/bin && ./spark-class org.apache.spark.deploy.master.Master --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

cd $SPARK_HOME/bin && ./spark-class org.apache.spark.deploy.worker.Worker --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG

elif [ "$SPARK_WORKLOAD" == "submit" ];
then
    echo "SPARK SUBMIT"
else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, submit"
fi

# Keep the container running
while true; do
  sleep 60
done