package com.drsoares.mirror.event;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The processing of messages need, in some use cases, to trigger some actions in case of something goes wrong or simply in certain
 * key parts of the state machine. To enable the hooking of behaviour we define here a set of keypoints by which the
 * callee applications can inject custom behaviour into the kafka processing stage
 */
public interface KafkaMirrorEvent {
    void onStop();
    void onStart();
    void onMessageSent(ProducerRecord producerRecord);
    void onMessageError(Throwable thrown);
}
