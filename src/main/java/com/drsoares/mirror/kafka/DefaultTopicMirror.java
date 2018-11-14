package com.drsoares.mirror.kafka;

import com.drsoares.mirror.TopicMirror;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DefaultTopicMirror implements TopicMirror {

    private static final String HEADER_KEY = "KafkaMirror";
    private static final Predicate<Header> CONTAINS_HEADER_PREDICATE = header -> header.key().equals(HEADER_KEY);

    private String hostname;

    private final Map<String, String> topicMapping;

    public DefaultTopicMirror() {
        this(new HashMap<>());
    }

    public DefaultTopicMirror(Map<String, String> topicMapping) {
        this.topicMapping = topicMapping;
    }

    @Override
    public List<ProducerRecord<byte[], byte[]>> handle(Iterable<ConsumerRecord<byte[], byte[]>> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .filter((record) ->
                        StreamSupport.stream(record.headers().spliterator(), false)
                                .noneMatch(CONTAINS_HEADER_PREDICATE)
                )
                .map((record) ->
                        new ProducerRecord<>(revolveTopic(record),
                                record.partition(),
                                record.key(),
                                record.value(),
                                Collections.singletonList(new RecordHeader(HEADER_KEY, getHostname().getBytes()))
                        )
                )
                .collect(Collectors.toList());
    }

    private String revolveTopic(ConsumerRecord<byte[], byte[]> record) {
        return Optional.ofNullable(topicMapping.get(record.topic())).orElse(record.topic());
    }

    private String getHostname() {
        if (Objects.isNull(hostname)) {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostname = "HostnameUnavailable";
            }
        }
        return hostname;
    }
}
