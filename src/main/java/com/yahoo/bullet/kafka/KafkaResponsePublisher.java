/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public class KafkaResponsePublisher implements Publisher {
    private final KafkaProducer<String, byte[]> producer;
    private final String responseTopic;
    private final boolean partitionRoutingEnabled;

    @Override
    public PubSubMessage send(PubSubMessage message) throws PubSubException {
        if (partitionRoutingEnabled) {
            TopicPartition responsePartition = getRouteInfo(message);
            producer.send(new ProducerRecord<>(responsePartition.topic(),
                                               responsePartition.partition(),
                                               message.getId(),
                                               SerializerDeserializer.toBytes(message)));
        } else {
            producer.send(new ProducerRecord<>(responseTopic, message.getId(), SerializerDeserializer.toBytes(message)));
        }
        return message;
    }


    @Override
    public void close() {
        producer.close();
    }

    private TopicPartition getRouteInfo(PubSubMessage message) throws PubSubException {
        try {
            KafkaMetadata metadata = (KafkaMetadata) message.getMetadata();
            return Objects.requireNonNull(metadata.getTopicPartition());
        } catch (Exception e) {
            throw new PubSubException("Invalid route information", e);
        }
    }
}

