#!/bin/sh

SLAVE_CNT=4

# make ssh key first
ssh-keygen -t rsa -P "" -f ./id_rsa
chmod 666 id_rsa.pub

ln id_rsa master/id_rsa
ln id_rsa.pub master/id_rsa.pub
ln id_rsa.pub slave/id_rsa.pub

ln hadoop-3.3.6.tar.gz master/hadoop-3.3.6.tar.gz
ln hadoop-3.3.6.tar.gz slave/hadoop-3.3.6.tar.gz

ln workers master/workers
ln workers slave/workers
# docker job

#   make network group
docker network create --driver=bridge hadoop-net
#   make slaves
docker build -t slave_img ./slave
for ((num=0; num<SLAVE_CNT; num++))
do
docker run -d --name hadoop-slave${num} --hostname slave${num} --network hadoop-net slave_img
done

#   make master
docker build -t master_img ./master

rm master/workers
rm slave/workers
rm slave/hadoop-3.3.6.tar.gz
rm master/hadoop-3.3.6.tar.gz
rm slave/id_rsa.pub
rm master/id_rsa.pub
rm master/id_rsa

docker run -itd --name hadoop-master --hostname master --network hadoop-net -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 9866:9866 -p 9867:9867 master_img bash