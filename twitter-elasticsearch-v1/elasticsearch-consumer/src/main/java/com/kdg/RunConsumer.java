package com.kdg;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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

        ElasticsearchClient elasticsearchClient = new ElasticsearchClient();
        RestHighLevelClient restHighLevelClient = elasticsearchClient.createClient();

        KafkaTwitterConsumer kafkaTwitterConsumer = new KafkaTwitterConsumer();
        KafkaConsumer<String, String> kafkaConsumer = kafkaTwitterConsumer.createConsumer();

        startConsume(restHighLevelClient, kafkaConsumer);
    }

    private static void startConsume(RestHighLevelClient restHighLevelClient, KafkaConsumer<String, String> kafkaConsumer) throws IOException {

        // Poll for new Data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            BulkRequest bulkRequest = new BulkRequest();

            int recordCount = records.count();
            logger.info("Received " + recordCount + " records...");

            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Idempotent Consumer - ElasticSearch won't insert duplicate ids
                    // If you haven't commit offset, then you can see duplicate feed IDs in logs, but ES won't inset those feeds in to cluster
                    String id = extractTweetId(record.value());
                    String indexName = "twitter";
                    String jsonStr = record.value();
                    bulkRequest.add(new IndexRequest(indexName).id(id).source(jsonStr, XContentType.JSON));
                } catch (NullPointerException e) {
                    logger.warn("skipped bad data " + record.value());
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            }

            logger.info("Committing offsets...");
            kafkaConsumer.commitSync();
            logger.info("Offsets have been commited...");

            try {
                Thread.sleep(1000); // Introduce a small delay
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
