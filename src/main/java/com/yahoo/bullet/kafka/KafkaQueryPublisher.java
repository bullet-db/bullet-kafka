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
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

import static com.yahoo.bullet.kafka.KafkaMetadata.getPartition;
import static com.yahoo.bullet.kafka.KafkaMetadata.setRouteData;

@Getter @RequiredArgsConstructor
public class KafkaQueryPublisher implements Publisher {
    private final KafkaProducer<String, byte[]> producer;
    private final List<TopicPartition> writePartitions;
    private final List<TopicPartition> receivePartitions;
    private final boolean partitionRoutingEnabled;

    @Override
    public PubSubMessage send(PubSubMessage message) throws PubSubException {
        TopicPartition requestPartition = getPartition(writePartitions, message);
        if (partitionRoutingEnabled) {
            setRouteData(receivePartitions, message);
        }
        producer.send(new ProducerRecord<>(requestPartition.topic(),
                                           requestPartition.partition(),
                                           message.getId(),
                                           SerializerDeserializer.toBytes(message)));
        return message;
    }

    @Override
    public void close() {
        producer.close();
    }
}
