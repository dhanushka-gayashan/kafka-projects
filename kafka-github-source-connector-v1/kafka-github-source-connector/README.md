## Run the connector - Standalone Mode

`docker pull landoop/fast-data-dev`

`docker-compose up kafka-cluster`

`./run.sh`


## Run the connector - Distributed Mode


`docker pull landoop/fast-data-dev`

`docker run -it --rm -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v <path_to_kafka-github-source-connector>/target/kafka-github-source-connector-1.0-SNAPSHOT-package/share/java/kafka-github-source-connector:/connectors/GitHub landoop/fast-data-dev`