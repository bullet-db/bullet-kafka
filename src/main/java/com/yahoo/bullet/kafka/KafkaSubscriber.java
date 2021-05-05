/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.BufferingSubscriber;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class KafkaSubscriber extends BufferingSubscriber {
    @Getter(AccessLevel.PACKAGE)
    private KafkaConsumer<String, byte[]> consumer;
    private boolean manualCommit;

    /**
     * Creates a KafkaSubscriber using a {@link KafkaConsumer}.
     *
     * @param consumer The {@link KafkaConsumer} to read data from.
     * @param maxUncommittedMessages The maximum number of messages that can be received before a commit is needed.
     * @param manualCommit Should this subscriber commit its offsets manually.
     */
    public KafkaSubscriber(KafkaConsumer<String, byte[]> consumer, int maxUncommittedMessages, boolean manualCommit) {
        super(maxUncommittedMessages);
        this.consumer = consumer;
        this.manualCommit = manualCommit;
    }

    /**
     *
     * Creates a KafkaSubscriber using a {@link KafkaConsumer} that does not manually commit.
     *
     * @param consumer The {@link KafkaConsumer} to read data from.
     * @param maxUncommittedMessages The maximum number of messages that can be received before a commit is needed.
     */
    public KafkaSubscriber(KafkaConsumer<String, byte[]> consumer, int maxUncommittedMessages) {
        this(consumer, maxUncommittedMessages, false);
    }

    /**
     * Creates a rate-limited KafkaSubscriber using a {@link KafkaConsumer}.
     *
     * @param consumer The {@link KafkaConsumer} to read data from.
     * @param maxUncommittedMessages The maximum number of messages that can be received before a commit is needed.
     * @param rateLimitMaxMessages The maximum number of messages that will be read in a rate limit interval.
     * @param rateLimitIntervalMS The duration of a rate limit interval in milliseconds.
     * @param manualCommit Should this subscriber commit its offsets manually.
     */
    public KafkaSubscriber(KafkaConsumer<String, byte[]> consumer, int maxUncommittedMessages, int rateLimitMaxMessages,
                           long rateLimitIntervalMS, boolean manualCommit) {
        super(maxUncommittedMessages, rateLimitMaxMessages, rateLimitIntervalMS);
        this.consumer = consumer;
        this.manualCommit = manualCommit;
    }

    /**
     * Creates a rate-limited KafkaSubscriber using a {@link KafkaConsumer} that doese not manually commit.
     *
     * @param consumer The {@link KafkaConsumer} to read data from.
     * @param maxUncommittedMessages The maximum number of messages that can be received before a commit is needed.
     * @param rateLimitMaxMessages The maximum number of messages that will be read in a rate limit interval.
     * @param rateLimitIntervalMS The duration of a rate limit interval in milliseconds.
     */
    public KafkaSubscriber(KafkaConsumer<String, byte[]> consumer, int maxUncommittedMessages, int rateLimitMaxMessages,
                           long rateLimitIntervalMS) {
        this(consumer, maxUncommittedMessages, rateLimitMaxMessages, rateLimitIntervalMS, false);
    }

    @Override
    public List<PubSubMessage> getMessages() throws PubSubException {
        ConsumerRecords<String, byte[]> buffer;
        try {
            buffer = consumer.poll(Duration.ZERO);
        } catch (KafkaException e) {
            throw new PubSubException("Consumer poll failed.", e);
        }
        List<PubSubMessage> messages = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> record : buffer) {
            messages.add(SerializerDeserializer.fromBytes(record.value()));
        }
        if (manualCommit) {
            consumer.commitAsync();
        }
        return messages;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
