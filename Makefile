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

initmysqldata:
	cd datagenerator && python3 main.py


loadDimDate:
	docker cp data/dim_date.csv hadoop-namenode:/tmp
	docker exec hadoop-namenode hdfs dfs -rm -r /tmp/dim_date.csv
	docker exec hadoop-namenode hdfs dfs -put /tmp/dim_date.csv /tmp
	docker exec scheduler spark-submit --master yarn --deploy-mode client /opt/airflow/dags/Transformation//get_dim_date.py

ddl-silver:
	docker exec scheduler spark-submit --master yarn --deploy-mode client /opt/airflow/dags/ddl/create_silver_tables.py

