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

import java.util.List;

import static com.yahoo.bullet.kafka.KafkaMetadata.getPartition;
import static com.yahoo.bullet.kafka.KafkaMetadata.getRouteInfo;

@Slf4j @RequiredArgsConstructor
public class KafkaResponsePublisher implements Publisher {
    private final KafkaProducer<String, byte[]> producer;
    private final List<TopicPartition> writePartitions;
    private final boolean partitionRoutingEnabled;

    @Override
    public PubSubMessage send(PubSubMessage message) throws PubSubException {
        TopicPartition responsePartition = partitionRoutingEnabled ? getRouteInfo(message) : getPartition(writePartitions, message);
        producer.send(new ProducerRecord<>(responsePartition.topic(), responsePartition.partition(),
                                           message.getId(), SerializerDeserializer.toBytes(message)));
        return message;
    }


    @Override
    public void close() {
        producer.close();
    }
}

