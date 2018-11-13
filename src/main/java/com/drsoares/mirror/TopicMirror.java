package com.drsoares.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public interface TopicMirror {
    List<ProducerRecord<byte[], byte[]>> handle(Iterable<ConsumerRecord<byte[], byte[]>> records);
}
