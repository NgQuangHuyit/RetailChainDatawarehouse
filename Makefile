build-hadoopbase:
	docker build -t hadoopbase:test -f ./containers/hadoop-base/Dockerfile .

build-sparkbase:
	docker build -t sparkbase -f ./containers/spark/Dockerfile .
up:
	docker-compose up -d

down:
	docker-compose down -v

initHiveMetastore:
	docker exec hive schematool -dbType postgres -initSchema

startHiveServer2:
	docker exec hive hiveserver2

startSparkHistoryServer:
	docker exec spark-master start-history-server.sh