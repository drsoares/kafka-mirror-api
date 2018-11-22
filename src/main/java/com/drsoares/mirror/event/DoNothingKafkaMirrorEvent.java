package com.drsoares.mirror.event;

import org.apache.kafka.clients.producer.ProducerRecord;

public class DoNothingKafkaMirrorEvent implements KafkaMirrorEvent {
    @Override
    public void onStop() {
        //This dummy implementation adopts a do nothing strategy
    }

    @Override
    public void onStart() {
        //This dummy implementation adopts a do nothing strategy
    }

    @Override
    public void onMessageSent(ProducerRecord producerRecord) {
        //This dummy implementation adopts a do nothing strategy
    }

    @Override
    public void onMessageError(Throwable thrown) {
        //This dummy implementation adopts a do nothing strategy
    }
}
