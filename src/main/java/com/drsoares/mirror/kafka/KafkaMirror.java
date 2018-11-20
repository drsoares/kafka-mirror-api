package com.drsoares.mirror.kafka;

import com.drsoares.mirror.Mirror;
import com.drsoares.mirror.RecordTransformer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class is a Kafka Mirror, it consumes records from a topic or multiple topics and publishes them to a correspondent one
 * into a target broker, or even the same broker.
 */
public class KafkaMirror implements Mirror {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMirror.class);
    private static final String DEFAULT_KAFKA_MIRROR_GROUP_ID = "kafka-mirror";

    private final String kafkaMirrorGroupId;

    private volatile boolean consuming;
    private volatile boolean ended;

    private final Set<String> topicsToSubscribe;
    private final String sourceBootStrapServers;
    private final String destinationBootStrapServers;
    private final RecordTransformer recordTransformer;

    /**
     * Constructor
     *
     * @param topicsToSubscribe           - a Set of topics to be mirrored
     * @param sourceBootStrapServers      - source bootstrap servers, from which data is consumed
     * @param destinationBootStrapServers - target bootstrap servers, for which data will be written
     * @param recordTransformer           - strategy to convert records from the source to the target
     * @param kafkaMirrorGroupId          - A Kafka Mirror consumer group id
     */
    public KafkaMirror(Set<String> topicsToSubscribe,
                       String sourceBootStrapServers,
                       String destinationBootStrapServers,
                       RecordTransformer recordTransformer,
                       String kafkaMirrorGroupId) {
        this.topicsToSubscribe = topicsToSubscribe;
        this.sourceBootStrapServers = sourceBootStrapServers;
        this.destinationBootStrapServers = destinationBootStrapServers;
        this.recordTransformer = recordTransformer;
        this.kafkaMirrorGroupId = kafkaMirrorGroupId;
    }

    /**
     * Constructor
     *
     * @param topicsToSubscribe           - a Set of topics to be mirrored
     * @param sourceBootStrapServers      - source bootstrap servers, from which data is consumed
     * @param destinationBootStrapServers - target bootstrap servers, for which data will be written
     * @param recordTransformer           - strategy to convert records from the source to the target
     */
    public KafkaMirror(Set<String> topicsToSubscribe,
                       String sourceBootStrapServers,
                       String destinationBootStrapServers,
                       RecordTransformer recordTransformer) {
        this(topicsToSubscribe, sourceBootStrapServers, destinationBootStrapServers, recordTransformer, DEFAULT_KAFKA_MIRROR_GROUP_ID);
    }

    Producer<byte[], byte[]> producer;
    Consumer<byte[], byte[]> consumer;

    /**
     * Starts the kafka mirror, it will consume the data from topics to subscribe and will publish them to another kafka broker
     * NOTICE: This method is a blocking method!
     */
    @Override
    public void start() {
        Consumer<byte[], byte[]> consumer = getConsumer();
        consumer.subscribe(topicsToSubscribe);
        consuming = true;
        do {
            try {
                for (ProducerRecord<byte[], byte[]> producerRecord : recordTransformer.handle(getConsumer().poll(100L))) {
                    getProducer().send(producerRecord).get();
                }
                getConsumer().commitSync();
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (Exception ex) {
                LOGGER.error("A failure occurred", ex);
            }
        } while (consuming);
        ended = true;
    }

    private Consumer<byte[], byte[]> getConsumer() {
        if (Objects.isNull(consumer)) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootStrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaMirrorGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumer = new KafkaConsumer<>(props);
        }
        return consumer;
    }

    private Producer<byte[], byte[]> getProducer() {
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
        consuming = false;
        while (!ended) {
            try {
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        getConsumer().close();
        getProducer().close();
    }
}
