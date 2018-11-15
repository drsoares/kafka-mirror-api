package com.drsoares.mirror.kafka;

import com.drsoares.mirror.TopicMirror;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.*;

class KafkaMirrorTest {

    @Test
    void kafkaMirrorShouldReplicateDataCrossBrokers() throws ExecutionException, InterruptedException, NoSuchFieldException {
        Producer<byte[], byte[]> producer = mock(Producer.class);
        Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        TopicMirror topicMirror = mock(TopicMirror.class);

        KafkaMirror kafkaMirror = new KafkaMirror(
                Collections.singleton("topic"),
                "sourbroker:9092",
                "destinationbroker:9092",
                topicMirror
        );
        kafkaMirror.consumer = consumer;
        kafkaMirror.producer = producer;

        when(consumer.poll(anyLong())).thenReturn(mock(ConsumerRecords.class));
        List<ProducerRecord<byte[], byte[]>> recordsToProduce = Collections.singletonList(mock(ProducerRecord.class));
        when(topicMirror.handle(anyIterable())).thenReturn(recordsToProduce);

        Future<RecordMetadata> future = mock(Future.class);
        doReturn(null).when(future).get();

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

}
