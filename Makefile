build-hadoopbase:
	docker build -t hadoopbase:test -f ./containers/hadoopbase/Dockerfile ./containers/hadoopbase

up:
	docker-compose up -d

down:
	docker-compose down -v

initHiveMetastore:
	docker exec hive schematool -dbType postgres -initSchema

startHiveServer2:
	docker exec hive hiveserver2

