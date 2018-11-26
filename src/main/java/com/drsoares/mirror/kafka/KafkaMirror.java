package com.drsoares.mirror.kafka;

import com.drsoares.mirror.Mirror;
import com.drsoares.mirror.RecordTransformer;
import com.drsoares.mirror.event.DoNothingKafkaMirrorEvent;
import com.drsoares.mirror.event.KafkaMirrorEvent;
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
    private static final KafkaMirrorEvent DEFAULT_KAFKA_MIRROR_EVENT = new DoNothingKafkaMirrorEvent();

    private volatile boolean consuming;
    private volatile boolean ended;

    private final Set<String> topicsToSubscribe;
    private final String sourceBootStrapServers;
    private final String destinationBootStrapServers;
    private final RecordTransformer recordTransformer;
    private final String kafkaMirrorGroupId;
    private final KafkaMirrorEvent eventHandler;


    public static class Builder {

        private Set<String> topicsToSubscribe;
        private String sourceBootStrapServers;
        private String destinationBootStrapServers;
        private RecordTransformer recordTransformer;
        private String kafkaMirrorGroupId = DEFAULT_KAFKA_MIRROR_GROUP_ID;
        private KafkaMirrorEvent eventHandler = DEFAULT_KAFKA_MIRROR_EVENT;

        public Builder() { }

        public Builder setTopicsToSubscribe(Set<String> topicsToSubscribe) {
            this.topicsToSubscribe = topicsToSubscribe;
            return this;
        }

        public Builder setSourceBootStrapServers(String sourceBootStrapServers) {
            this.sourceBootStrapServers = sourceBootStrapServers;
            return this;
        }

        public Builder setDestinationBootStrapServers(String destinationBootStrapServers) {
            this.destinationBootStrapServers = destinationBootStrapServers;
            return this;
        }

        public Builder setRecordTransformer(RecordTransformer recordTransformer) {
            this.recordTransformer = recordTransformer;
            return this;
        }

        public Builder setKafkaMirrorGroupId(String kafkaMirrorGroupId) {
            this.kafkaMirrorGroupId = kafkaMirrorGroupId;
            return this;
        }

        public Builder setEventHandler(KafkaMirrorEvent eventHandler) {
            this.eventHandler = eventHandler;
            return this;
        }

        public KafkaMirror build() {
            return new KafkaMirror(
                    topicsToSubscribe,
                    sourceBootStrapServers,
                    destinationBootStrapServers,
                    recordTransformer,
                    kafkaMirrorGroupId,
                    eventHandler
            );
        }

    }


    /**
     * Constructor
     *
     * @param topicsToSubscribe           - a Set of topics to be mirrored
     * @param sourceBootStrapServers      - source bootstrap servers, from which data is consumed
     * @param destinationBootStrapServers - target bootstrap servers, for which data will be written
     * @param recordTransformer           - strategy to convert records from the source to the target
     * @param kafkaMirrorGroupId          - consumer group id used by the mirror
     * @param eventHandler                - set of callbacks for key points of the mirroring process
     */
    public KafkaMirror(Set<String> topicsToSubscribe,
                       String sourceBootStrapServers,
                       String destinationBootStrapServers,
                       RecordTransformer recordTransformer,
                       String kafkaMirrorGroupId,
                       KafkaMirrorEvent eventHandler) {
        this.topicsToSubscribe = topicsToSubscribe;
        this.sourceBootStrapServers = sourceBootStrapServers;
        this.destinationBootStrapServers = destinationBootStrapServers;
        this.recordTransformer = recordTransformer;
        this.kafkaMirrorGroupId = kafkaMirrorGroupId;
        this.eventHandler = eventHandler;
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
        this(
                topicsToSubscribe,
                sourceBootStrapServers,
                destinationBootStrapServers,
                recordTransformer,
                DEFAULT_KAFKA_MIRROR_GROUP_ID,
                DEFAULT_KAFKA_MIRROR_EVENT
        );
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
        eventHandler.onStart();
        do {
            try {
                for (ProducerRecord<byte[], byte[]> producerRecord : recordTransformer.handle(getConsumer().poll(100L))) {
                    getProducer().send(producerRecord).get();
                    eventHandler.onMessageSent(producerRecord);
                }
                getConsumer().commitSync();
                TimeUnit.MILLISECONDS.sleep(10L);
            } catch (Throwable throwable) {
                LOGGER.error("A failure occurred", throwable);
                eventHandler.onMessageError(throwable);

            }
        } while (consuming);
        ended = true;
        eventHandler.onStop();
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
