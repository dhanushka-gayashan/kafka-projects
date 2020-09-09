> Create Kafka Topic

    kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets --create --partitions 6 --replication-factor 1

> Test Kafka Consumer

    kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets

