package com.drsoares.kafka.mirror;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Mirror {

    private volatile boolean consuming;

    private final String whitelist;
    private final String sourceBootStrapServers;
    private final String destinationBootStrapServers;
    private final TopicMirror topicMirror;

    public Mirror(String whitelist,
                  String sourceBootStrapServers,
                  String destinationBootStrapServers,
                  TopicMirror topicMirror) {
        this.whitelist = whitelist;
        this.sourceBootStrapServers = sourceBootStrapServers;
        this.destinationBootStrapServers = destinationBootStrapServers;
        this.topicMirror = topicMirror;
    }

    private void run() throws ExecutionException, InterruptedException {
        List<String> topics = getAdminClient().listTopics()
                .names().get()
                .stream()
                .filter(name -> name.matches(whitelist))
                .collect(Collectors.toList());

        Consumer<byte[], byte[]> consumer = getConsumer();
        consumer.subscribe(topics);

        consuming = true;
        while (consuming) {
            for (ProducerRecord<byte[], byte[]> producerRecord : topicMirror.handle(consumer.poll(1000L))) {
                getProducer().send(producerRecord).get();
            }
            TimeUnit.MILLISECONDS.sleep(10L); //TODO - review value
        }
    }

    private Consumer<byte[], byte[]> getConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootStrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-mirror");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); //FIXME
        return new KafkaConsumer<>(props);
    }

    private KafkaProducer<byte[], byte[]> getProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private AdminClient getAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootStrapServers);
        return AdminClient.create(properties);
    }

    public void stop() {
        getConsumer().close(); //TODO - consider using pause
        getProducer().close();
        consuming = false;
    }
}
