package com.kdg;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaTwitterConsumer {

    public KafkaConsumer<String, String> createConsumer() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "twitter-kafka-elasticsearch";
        String topic = "twitter_tweets";

        // Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest / latest / none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to Single Topic
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
}
