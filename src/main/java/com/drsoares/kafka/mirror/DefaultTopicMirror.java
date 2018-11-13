package com.drsoares.kafka.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DefaultTopicMirror implements TopicMirror {

    private static final String HEADER_KEY = "Mirror";
    private static final Predicate<Header> CONTAINS_HEADER_PREDICATE = header -> header.key().equals(HEADER_KEY);

    private String hostname;

    @Override
    public List<ProducerRecord<byte[], byte[]>> handle(Iterable<ConsumerRecord<byte[], byte[]>> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .filter((record) -> StreamSupport.stream(record.headers().spliterator(), false).noneMatch(CONTAINS_HEADER_PREDICATE))
                .map((record) -> new ProducerRecord<>(record.topic(), record.partition(), record.key(), record.value(), Arrays.asList(new RecordHeader(HEADER_KEY, getHostname().getBytes()))))
                .collect(Collectors.toList());
    }

    private String getHostname() {
        if (hostname != null) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostname = "HostnameUnavailable";
            }
        }
        return hostname;
    }
}
