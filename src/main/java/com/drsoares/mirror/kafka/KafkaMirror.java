package com.drsoares.mirror.kafka;

import com.drsoares.mirror.Mirror;
import com.drsoares.mirror.TopicMirror;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaMirror implements Mirror {

    private volatile boolean consuming;

    private final Set<String> topicsToSubscribe;
    private final String sourceBootStrapServers;
    private final String destinationBootStrapServers;
    private final TopicMirror topicMirror;

    public KafkaMirror(Set<String> topicsToSubscribe,
                String sourceBootStrapServers,
                String destinationBootStrapServers,
                TopicMirror topicMirror) {
        this.topicsToSubscribe = topicsToSubscribe;
        this.sourceBootStrapServers = sourceBootStrapServers;
        this.destinationBootStrapServers = destinationBootStrapServers;
        this.topicMirror = topicMirror;
    }

    private KafkaProducer producer;
    private KafkaConsumer consumer;

    @Override
    public void start() {
        try {
            Consumer<byte[], byte[]> consumer = getConsumer();
            consumer.subscribe(topicsToSubscribe);

            consuming = true;
            while (consuming) {
                for (ProducerRecord<byte[], byte[]> producerRecord : topicMirror.handle(consumer.poll(1000L))) {
                    getProducer().send(producerRecord).get();
                }
                getConsumer().commitSync();
                TimeUnit.MILLISECONDS.sleep(10L); //TODO - review value
            }
        } catch (ExecutionException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Consumer<byte[], byte[]> getConsumer() {
        if (Objects.isNull(consumer)) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootStrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-mirror");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumer = new KafkaConsumer<>(props);
        }
        return consumer;
    }

    private KafkaProducer<byte[], byte[]> getProducer() {
        if (Objects.isNull(producer)) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBootStrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            producer = new KafkaProducer<>(props);
        }
        return producer;
    }

    @Override
    public void stop() {
        getConsumer().close();
        getProducer().close();
        consuming = false;
    }
}
