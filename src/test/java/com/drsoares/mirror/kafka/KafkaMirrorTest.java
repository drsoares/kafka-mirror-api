package com.drsoares.mirror.kafka;

import com.drsoares.mirror.Mirror;
import com.google.common.collect.Sets;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaMirrorTest {

    @RegisterExtension
    public static final SharedKafkaTestResource kafka = new SharedKafkaTestResource().withBrokers(1);

    @Test
    void kafkaMirrorShouldReplicateDataCrossBrokers() throws ExecutionException, InterruptedException {
        Map<String, String> map = new HashMap<>();
        map.put("source", "mirror");

        Mirror kafkaMirror = new KafkaMirror(Sets.newHashSet("source"), kafka.getKafkaConnectString(), kafka.getKafkaConnectString(), new DefaultTopicMirror(map));
        new Thread(kafkaMirror::start).start();

        KafkaProducer<String, String> producer = getProducer();
        producer.send(new ProducerRecord<>("source", "key1", "value1")).get();
        producer.send(new ProducerRecord<>("source", "key2", "value2")).get();
        producer.send(new ProducerRecord<>("source", "key3", "value3")).get();
        producer.send(new ProducerRecord<>("source", "key4", "value4")).get();

        TimeUnit.SECONDS.sleep(5L);

        kafkaMirror.stop();

        Consumer<String, String> consumer = getConsumer();
        consumer.subscribe(Collections.singleton("mirror"));
        ConsumerRecords<String, String> records = consumer.poll(100L);

        assertEquals(4, records.count());
    }

    private KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
        return kafka.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class, props);
    }

    private KafkaConsumer<String, String> getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getKafkaConnectString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_kafka_mirror");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return kafka.getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, StringDeserializer.class, props);
    }

}
