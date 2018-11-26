package com.drsoares.mirror.kafka;

import com.drsoares.mirror.RecordTransformer;
import com.drsoares.mirror.event.DoNothingKafkaMirrorEvent;
import com.drsoares.mirror.event.KafkaMirrorEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.*;

class KafkaMirrorTest {

    private static final RecordMetadata DUMMY_RECORD_METADATA = new RecordMetadata(new TopicPartition("topic", 1), 0L, 0L, System.currentTimeMillis(),
            0L, 0, 0);

    @Test
    void kafkaMirrorShouldReplicateDataCrossBrokers() throws ExecutionException, InterruptedException {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        RecordTransformer recordTransformer = mock(RecordTransformer.class);

        KafkaMirror kafkaMirror = new KafkaMirror.Builder()
                .setTopicsToSubscribe(Collections.singleton("topic"))
                .setSourceBootStrapServers("sourbroker:9092")
                .setDestinationBootStrapServers("destinationbroker:9092")
                .setRecordTransformer(recordTransformer)
                .build();

        kafkaMirror.consumer = consumer;
        kafkaMirror.producer = producer;

        when(consumer.poll(anyLong())).thenReturn(mock(ConsumerRecords.class));
        List<ProducerRecord<byte[], byte[]>> recordsToProduce = Collections.singletonList(mock(ProducerRecord.class));
        when(recordTransformer.handle(anyIterable())).thenReturn(recordsToProduce);

        Future<RecordMetadata> future = mock(Future.class);
        doReturn(DUMMY_RECORD_METADATA).when(future).get();

        doNothing().when(consumer).subscribe(anyCollection());
        doNothing().when(consumer).commitSync();
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        new Thread(kafkaMirror::start).start();

        TimeUnit.MILLISECONDS.sleep(100L);

        kafkaMirror.stop();

        verify(consumer, atLeastOnce()).poll(anyLong());
        verify(consumer, atLeastOnce()).commitSync();
        verify(producer, atLeastOnce()).send(any(ProducerRecord.class));
    }

    @Test
    void kafkaMirrorShouldBeResilientToSourceBrokersCallTransientFailures() throws ExecutionException, InterruptedException {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        RecordTransformer recordTransformer = mock(RecordTransformer.class);

        KafkaMirror kafkaMirror = new KafkaMirror.Builder()
                .setTopicsToSubscribe(Collections.singleton("topic"))
                .setSourceBootStrapServers("sourbroker:9092")
                .setDestinationBootStrapServers("destinationbroker:9092")
                .setRecordTransformer(recordTransformer)
                .build();
        kafkaMirror.consumer = consumer;
        kafkaMirror.producer = producer;

        when(consumer.poll(anyLong()))
                .thenThrow(RuntimeException.class)
                .thenThrow(RuntimeException.class)
                .thenReturn(mock(ConsumerRecords.class));

        List<ProducerRecord<byte[], byte[]>> recordsToProduce = Collections.singletonList(mock(ProducerRecord.class));
        when(recordTransformer.handle(anyIterable())).thenReturn(recordsToProduce);

        Future<RecordMetadata> future = mock(Future.class);
        doReturn(DUMMY_RECORD_METADATA).when(future).get();

        doNothing().when(consumer).subscribe(anyCollection());
        doNothing().when(consumer).commitSync();
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        new Thread(kafkaMirror::start).start();

        TimeUnit.MILLISECONDS.sleep(100L);

        kafkaMirror.stop();

        verify(consumer, atLeastOnce()).poll(anyLong());
        verify(consumer, atLeastOnce()).commitSync();
        verify(producer, atLeastOnce()).send(any(ProducerRecord.class));
    }

    @Test
    void kafkaMirrorShouldBeResilientToDestinationBrokersCallTransientFailures() throws ExecutionException, InterruptedException {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        RecordTransformer recordTransformer = mock(RecordTransformer.class);

        KafkaMirror kafkaMirror = new KafkaMirror.Builder()
                .setTopicsToSubscribe(Collections.singleton("topic"))
                .setSourceBootStrapServers("sourbroker:9092")
                .setDestinationBootStrapServers("destinationbroker:9092")
                .setRecordTransformer(recordTransformer)
                .build();

        kafkaMirror.consumer = consumer;
        kafkaMirror.producer = producer;

        when(consumer.poll(anyLong())).thenReturn(mock(ConsumerRecords.class));

        List<ProducerRecord<byte[], byte[]>> recordsToProduce = Collections.singletonList(mock(ProducerRecord.class));
        when(recordTransformer.handle(anyIterable())).thenReturn(recordsToProduce);

        Future<RecordMetadata> future = mock(Future.class);
        when(future.get()).thenReturn(DUMMY_RECORD_METADATA);

        doNothing().when(consumer).subscribe(anyCollection());
        doNothing().when(consumer).commitSync();
        when(producer.send(any(ProducerRecord.class)))
                .thenThrow(RuntimeException.class)
                .thenThrow(RuntimeException.class)
                .thenReturn(future);

        new Thread(kafkaMirror::start).start();

        TimeUnit.MILLISECONDS.sleep(100L);

        kafkaMirror.stop();

        verify(consumer, atLeastOnce()).poll(anyLong());
        verify(consumer, atLeastOnce()).commitSync();
        verify(producer, atLeastOnce()).send(any(ProducerRecord.class));
    }
}
