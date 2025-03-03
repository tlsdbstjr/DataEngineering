#! /bin/sh
SLAVE_CNT=4

# make network groups
docker network create --driver=bridge hadoop-net

for ((num=0; num<SLAVE_CNT; num++))
do
docker container create --name spark-slave${num} --hostname slave${num} --network hadoop-net slave_spark
done

docker container create --name spark-master --hostname master --network hadoop-net -p 8888:8888 -p 8088:8088 -p 9870:9870 -p 8080:8080 -p 4040:4040 -p 7077:7077 -p 8081:8081 master_spark
