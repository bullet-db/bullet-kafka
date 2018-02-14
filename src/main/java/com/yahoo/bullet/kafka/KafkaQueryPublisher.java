/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;

@Getter @RequiredArgsConstructor
public class KafkaQueryPublisher implements Publisher {
    private final KafkaProducer<String, byte[]> producer;
    private final List<TopicPartition> writePartitions;
    private final List<TopicPartition> receivePartitions;

    /**
     * Set metadata required to route responses.
     *
     * @param message the {@link PubSubMessage} to set metadata to.
     */
    private void setRouteData(PubSubMessage message) throws PubSubException {
        try {
            TopicPartition responsePartition = getPartition(receivePartitions, message);
            message.getMetadata().setContent(responsePartition);
        } catch (Exception e) {
            throw new PubSubException("Could not set route metadata. ", e);
        }
    }

    @Override
    public void send(String id, String content) throws PubSubException {
        send(new PubSubMessage(id, content, new Metadata()));
    }

    @Override
    public void send(PubSubMessage message) throws PubSubException {
        TopicPartition requestPartition = getPartition(writePartitions, message);
        setRouteData(message);
        producer.send(new ProducerRecord<>(requestPartition.topic(), requestPartition.partition(), message.getId(),
                                            SerializerDeserializer.toBytes(message)));
    }

    @Override
    public void close() {
        producer.close();
    }

    private TopicPartition getPartition(List<TopicPartition> partitionList, PubSubMessage message) {
        int partitionIndex = Math.abs(message.getId().hashCode() % partitionList.size());
        return partitionList.get(partitionIndex);
    }
}
