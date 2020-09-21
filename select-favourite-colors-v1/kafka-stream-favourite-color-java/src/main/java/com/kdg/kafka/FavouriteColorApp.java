package com.kdg.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {

    private static Properties createConfig() {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Disable the Cache to demonstrate all the "steps" involved in the transformation - Not Recommended in PROD
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return config;
    }

    private static Topology createTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> usersColorsInput = builder.stream("users-colors-input");
        KStream<String, String> usersColorsOutput = usersColorsInput
                .filter((user, color) -> !user.trim().isEmpty() && !color.trim().isEmpty())
                //.map((user, color) -> KeyValue.pair(user.trim().toLowerCase(), color.trim().toLowerCase()))
                .selectKey((user, color) -> user.trim().toLowerCase())
                .mapValues(color -> color.trim().toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "red", "blue").contains(color)); // Select only records with given colors.
        usersColorsOutput.to("users-colors-output");

        KTable<String, String> usersColorsOut = builder.table("users-colors-output");
        KTable<String, Long> favouriteColors = usersColorsOut
                //.filter((user, color) -> Arrays.asList("green", "red", "blue").contains(color)) //Depend on Use-Case
                .groupBy((user, color) -> KeyValue.pair(color, color))
                .count(
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColors")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                );
        favouriteColors.toStream().to("favourite-colours", Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {

        // Create Stream
        KafkaStreams streams = new KafkaStreams(createTopology(), createConfig());

        // Only do this in DEV - Not in PROD
        streams.cleanUp();

        // Start Kafka Stream
        streams.start();

        // Shutdown the application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Print the Topology
        System.out.println(streams.toString());

        // Learning Purpose - Print the Topology every 5 seconds
        while (true) {
            streams.localThreadsMetadata().forEach(System.out::println);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
