#!/bin/bash

dataDir=/hadoop-data/dfs/data

if [ ! -d $dataDir ]; then
  echo "Datanode data directory not found: $dataDir"
  exit 2
fi

service ssh start

while true; do
  sleep 60
done