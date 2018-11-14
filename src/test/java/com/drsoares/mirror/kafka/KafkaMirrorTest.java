package com.drsoares.mirror.kafka;

import com.drsoares.mirror.Mirror;
import com.google.common.collect.Sets;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
    public static final SharedKafkaTestResource source = new SharedKafkaTestResource().withBrokers(1);

    @Test
    void kafkaMirrorShouldReplicateDataCrossBrokers() throws ExecutionException, InterruptedException {
        source.getKafkaBrokers().forEach(kafkaBroker -> {
            try {
                kafkaBroker.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        source.getKafkaTestUtils().createTopic("source", 2, (short) 1);
        source.getKafkaTestUtils().createTopic("mirror", 2, (short) 1);

        Map<String, String> map = new HashMap<>();
        map.put("source", "mirror");

        Mirror kafkaMirror = new KafkaMirror(Sets.newHashSet("source"), source.getKafkaConnectString(), source.getKafkaConnectString(), new DefaultTopicMirror(map));
        Thread thread = new Thread(kafkaMirror::start);
        thread.start();

        source.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class).send(new ProducerRecord<>("source", "key1", "value1")).get();
        source.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class).send(new ProducerRecord<>("source", "key2", "value2")).get();
        source.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class).send(new ProducerRecord<>("source", "key3", "value3")).get();
        source.getKafkaTestUtils().getKafkaProducer(StringSerializer.class, StringSerializer.class).send(new ProducerRecord<>("source", "key4", "value4")).get();

        TimeUnit.MILLISECONDS.sleep(1000L);

        kafkaMirror.stop();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, source.getKafkaConnectString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_kafka_mirror");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        Consumer<String, String> consumer = source.getKafkaTestUtils().getKafkaConsumer(StringDeserializer.class, StringDeserializer.class, props);
        consumer.subscribe(Collections.singleton("mirror"));
        ConsumerRecords<String, String> records = consumer.poll(100L);

        assertEquals(1, records.count());

        source.getKafkaBrokers().forEach(kafkaBroker -> {
            try {
                kafkaBroker.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

}
