/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaSubscriber implements Subscriber {
    @Getter(AccessLevel.PACKAGE)
    private KafkaConsumer<String, byte[]> consumer;
    private int maxUnackedMessages;
    private List<PubSubMessage> receivedMessages;
    private Map<Pair<String, Integer>, PubSubMessage> unackedMessages;
    private ConsumerRecords<String, byte[]> buffer;

    /**
     * Creates a KafkaSubscriber using a {@link KafkaConsumer}.
     *
     * @param consumer The {@link KafkaConsumer} to read data from.
     * @param maxUnackedMessages The maximum number of messages that can be received before an ack is needed.
     */
    public KafkaSubscriber(KafkaConsumer<String, byte[]> consumer, int maxUnackedMessages) {
        this.consumer = consumer;
        this.maxUnackedMessages = maxUnackedMessages;
        this.receivedMessages = new LinkedList<>();
        this.unackedMessages = new HashMap<>();
    }

    /**
     * Get the next batch of messages from Kafka and store it in the local buffer.
     *
     * @throws PubSubException when Kafka throws an error.
     */
    private boolean getMessages() throws PubSubException {
        consumer.commitSync();
        try {
            buffer = consumer.poll(0);
        } catch (KafkaException e) {
            throw new PubSubException("Consumer poll failed", e);
        }
        for (ConsumerRecord<String, byte[]> record : buffer) {
            Object message = SerializerDeserializer.fromBytes(record.value());
            if (message == null || !(message instanceof PubSubMessage)) {
                log.warn("Invalid message received: {}", message);
                continue;
            }
            receivedMessages.add((PubSubMessage) message);
        }
        return !receivedMessages.isEmpty();
    }

    @Override
    public PubSubMessage receive() throws PubSubException {
        if (unackedMessages.size() >= maxUnackedMessages) {
            log.warn("Reached limit of max unacked messages: {}. Waiting for acks to proceed.", maxUnackedMessages);
            return null;
        }
        if (receivedMessages.isEmpty() && !getMessages()) {
            return null;
        }
        PubSubMessage message = receivedMessages.remove(0);
        unackedMessages.put(ImmutablePair.of(message.getId(), message.getSequence()), message);
        return message;
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public void commit(String id, int sequence) {
        ImmutablePair<String, Integer> key = ImmutablePair.of(id, sequence);
        unackedMessages.remove(key);
    }

    @Override
    public void fail(String id, int sequence) {
        Pair<String, Integer> compositeID = ImmutablePair.of(id, sequence);
        PubSubMessage message = unackedMessages.get(compositeID);
        if (message != null) {
            receivedMessages.add(0, message);
            unackedMessages.remove(compositeID);
        }
    }
}
