package com.kdg;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class RunConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RunConsumer.class.getName());

    public static void main(String[] args) throws IOException {

        RestHighLevelClient elasticsearchClient = new ElasticsearchClient().createClient();

        String topic = "twitter_tweets";
        KafkaConsumer<String, String> kafkaTwitterConsumer = new KafkaTwitterConsumer().createConsumer(topic);

        startConsume(kafkaTwitterConsumer, elasticsearchClient);
    }

    private static void startConsume(KafkaConsumer<String, String> kafkaTwitterConsumer, RestHighLevelClient elasticsearchClient) throws IOException {

        // Poll for new Data
        while (true) {
            ConsumerRecords<String, String> records = kafkaTwitterConsumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records...");

            if (recordCount > 0) {
                BulkRequest bulkRequest = new BulkRequest();

                records.forEach(record -> {
                    try {
                        String indexName = "twitter";

                        String jsonStr = record.value();

                        // Idempotent Consumer - ElasticSearch won't insert duplicate ids
                        // If you haven't commit offset, then you can see duplicate feed IDs in logs, but ES won't inset those feeds in to cluster
                        String id = extractTweetId(jsonStr);

                        IndexRequest indexRequest = new IndexRequest(indexName);
                        indexRequest.id(id);
                        indexRequest.source(jsonStr, XContentType.JSON);
                        bulkRequest.add(indexRequest);
                    } catch (NullPointerException e) {
                        logger.warn("skipped bad data " + record.value());
                    }
                });

                // Sent Bulk Request
                BulkResponse bulkItemResponses = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committed Bulk Tweets: " + bulkItemResponses.status().toString());

                // Committing Offsets
                logger.info("Committing offsets...");
                kafkaTwitterConsumer.commitSync();
                logger.info("Offsets have been committed...");
            }

            try {
                // Introduce a small delay
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static String extractTweetId(String tweetJson) throws NullPointerException{
        return JsonParser.parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
