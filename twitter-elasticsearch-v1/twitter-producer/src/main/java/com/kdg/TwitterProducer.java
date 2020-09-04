package com.kdg;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public void run() {
        logger.info("Start Twitter Client");

        final String kafkaTopic = "twitter_tweets";

        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        // Create and Connect Twitter Client
        Client hosebirdClient = createTwitterClient(msgQueue);
        hosebirdClient.connect();

        // Create Kafka Producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // Add Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application....");

            logger.info("shutting down client from twitter...");
            hosebirdClient.stop();

            logger.info("closing kafka producer...");
            kafkaProducer.close();

            logger.info("done!...");
        }));

        // Loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String msg = null;

            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if (msg != null) {
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Something bad happened...", e);
                    }
                });
            }
        }

        logger.info("End of Application");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

        final String consumerKey = "";
        final String consumerSecret = "";
        final String token = "";
        final String tokenSecret = "";

        // Following Terms
        List<String> terms = Lists.newArrayList("aws", "kafka", "cnn");

        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // Create and Return Twitter Client
        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        final String bootstrapServers = "127.0.0.1:9092";

        // Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High Throughput Producer (Batching) - at the expense of a bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // Create and Return Producer
        return new KafkaProducer<>(properties);
    }
}
