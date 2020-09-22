package com.kdg.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class TransactionsProducer {

    private static Properties createConfig() {
        Properties config = new Properties();

        // Kafka Bootstrap Server
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer ACKS
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");

        // Leverage Idempotent Producer
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return config;
    }

    static ProducerRecord<String, String> createTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        transaction.put("name", name);
        transaction.put("amount", ThreadLocalRandom.current().nextInt(0, 100));
        transaction.put("time", Instant.now().toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }

    public static void main(String[] args) {
        Producer<String, String> producer = new KafkaProducer<>(createConfig());

        int batch = 0;

        while (true) {

            System.out.println("Producing Batch: " + batch);

            try {
                producer.send(createTransaction("john"));
                Thread.sleep(100);

                producer.send(createTransaction("mark"));
                Thread.sleep(100);

                producer.send(createTransaction("alice"));
                Thread.sleep(100);
            }catch (InterruptedException e) {
                break;
            }
        }

        producer.close();
    }
}
