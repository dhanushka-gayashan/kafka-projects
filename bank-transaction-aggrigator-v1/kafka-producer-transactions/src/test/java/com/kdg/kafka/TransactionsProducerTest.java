package com.kdg.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TransactionsProducerTest {

    @Test
    public void createTransactionTest() {

        ProducerRecord<String, String> record = TransactionsProducer.createTransaction("john");

        String key = record.key();
        Assert.assertEquals(key, "john");

        String value = record.value();
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            Assert.assertEquals(node.get("name").asText(), "john");
            Assert.assertTrue("Amount shoue be less than 100", node.get("amount").asInt() < 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(value);
    }
}
