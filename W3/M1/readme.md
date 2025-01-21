# 개요
hadoop을 single-node로 실행해보는 파트입니다.

# 실행방법
0. M1 폴더로 이동합니다
1. 하둡 파일을 다운로드 받습니다.
```sh
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```
2. 이미지를 빌드합니다.
```sh
docker image build -t hd-single .
```
3. 컨테이너를 만들고 실행합니다.
```sh
docker run -itd --name hadoop-master --hostname master --network hadoop-net -p 8088:8088 -p 9870:9870 -p 9864:9864 -p 9866:9866 -p 9867:9867 master_img bash
```
4. (이후 수정 예정) 실행중인 컨테이너에서 ssh를 시작합니다.
```sh
service ssh start
````
5. (이후 수정 예정) hdfs를 포멧합니다
```sh
hdfs namenode -format
````
6. (이후 수정 예정) hdfs를 켭니다.
```sh
start-dfs.sh
````
7. (이후 수정 예정) yarn을 켭니다.
```sh
start-yarn.sh
```
8. 브라우저를 localhost:8088혹은 localhost:9870을 접속합니다.