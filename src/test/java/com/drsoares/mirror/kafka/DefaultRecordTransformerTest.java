package com.drsoares.mirror.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class DefaultRecordTransformerTest {

    @Test
    void topicMirrorShouldTransformConsumedRecordsInProducedRecordsWithAKafkaMirrorHeader() {
        DefaultRecordTransformer defaultTopicMirror = new DefaultRecordTransformer();

        String key = "key";
        String value = "value";
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 100L, key.getBytes(), value.getBytes());

        List<ProducerRecord<byte[], byte[]>> producerRecordList = defaultTopicMirror.handle(Collections.singletonList(consumerRecord));

        assertFalse(producerRecordList.isEmpty());
        producerRecordList.forEach(record -> {
            assertEquals(record.topic(), consumerRecord.topic());
            assertTrue(record.partition() == consumerRecord.partition());
            assertEquals(record.key(), consumerRecord.key());
            assertEquals(record.value(), consumerRecord.value());
            assertTrue(StreamSupport.stream(record.headers().spliterator(), false).anyMatch(header -> header.key().equals("KafkaMirror")));
        });
    }

    @Test
    void topicMirrorShouldTransformConsumedRecordsInProducedRecordsWithAKafkaMirrorHeaderToUsingTheTopicMapping() {
        Map<String, String> topicMapping = new HashMap<>();
        topicMapping.put("input", "output");
        DefaultRecordTransformer defaultTopicMirror = new DefaultRecordTransformer(topicMapping);

        String key = "key";
        String value = "value";
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("input", 1, 100L, key.getBytes(), value.getBytes());

        List<ProducerRecord<byte[], byte[]>> producerRecordList = defaultTopicMirror.handle(Collections.singletonList(consumerRecord));

        assertFalse(producerRecordList.isEmpty());
        producerRecordList.forEach(record -> {
            assertEquals(record.topic(), "output");
            assertTrue(record.partition() == consumerRecord.partition());
            assertEquals(record.key(), consumerRecord.key());
            assertEquals(record.value(), consumerRecord.value());
            assertTrue(StreamSupport.stream(record.headers().spliterator(), false).anyMatch(header -> header.key().equals("KafkaMirror")));
        });
    }

    @Test
    void topicMirrorShouldIgnoreMirroredRecords() {
        DefaultRecordTransformer defaultTopicMirror = new DefaultRecordTransformer();

        String key = "key";
        String value = "value";
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 100L, key.getBytes(), value.getBytes());
        consumerRecord.headers().add("KafkaMirror", "hostname".getBytes());

        List<ProducerRecord<byte[], byte[]>> producerRecordList = defaultTopicMirror.handle(Collections.singletonList(consumerRecord));

        assertTrue(producerRecordList.isEmpty());
    }
}
