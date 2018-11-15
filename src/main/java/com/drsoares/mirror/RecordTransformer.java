package com.drsoares.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public interface RecordTransformer {
    /**
     * Converts records consumed from one topic into producer records
     * @param records - records consumed from the topic that should be mirrored
     * @return a list of producer records to be written to the target topic
     */
    List<ProducerRecord<byte[], byte[]>> handle(Iterable<ConsumerRecord<byte[], byte[]>> records);
}
