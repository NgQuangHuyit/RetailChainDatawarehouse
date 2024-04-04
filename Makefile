start-ssh-master:
	docker-compose exec hadoop-namenode /bin/bash -c "service ssh start"

start-ssh-slave1:
	docker-compose exec hadoop-datanode01 /bin/bash -c "service ssh start"


start-ssh: start-ssh-master start-ssh-slave1