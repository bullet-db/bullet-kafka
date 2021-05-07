/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class KafkaMetadata extends Metadata implements Serializable {
    private static final long serialVersionUID = 4657220477717531260L;

    @Getter @Setter
    private TopicPartition topicPartition;

    /**
     * Constructor that wraps a {@link Metadata} and {@link TopicPartition}.
     *
     * @param metadata The metadata that is to be wrapped.
     * @param topicPartition The {@link TopicPartition} information for Kafka to use.
     */
    public KafkaMetadata(Metadata metadata, TopicPartition topicPartition) {
        super(metadata.getSignal(), metadata.getContent());
        this.topicPartition = topicPartition;
    }

    /**
     * Constructor for just a {@link TopicPartition}.
     *
     * @param topicPartition The {@link TopicPartition} information for Kafka to use.
     */
    public KafkaMetadata(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    @Override
    public Metadata copy() {
        return new KafkaMetadata(this, topicPartition);
    }

    /**
     * Gets a {@link TopicPartition} from the given {@link List} for a particular {@link PubSubMessage}.
     *
     * @param partitionList The {@link List} of {@link TopicPartition}.
     * @param message The {@link PubSubMessage} to deliver to one of the {@link TopicPartition}.
     * @return A {@link TopicPartition} to use from the given partitions.
     */
    public static TopicPartition getPartition(List<TopicPartition> partitionList, PubSubMessage message) {
        int partitionIndex = Math.abs(message.getId().hashCode() % partitionList.size());
        return partitionList.get(partitionIndex);
    }

    /**
     * Given a {@link List} of {@link TopicPartition}, set metadata required to route responses into the
     * {@link PubSubMessage}. For a given message and partitions, this method always picks the same partition.
     *
     * @param partitions The {@link List} of {@link TopicPartition}.
     * @param message The {@link PubSubMessage} to set metadata to.
     * @throws PubSubException if there were issues setting the metadata.
     */
    public static void setRouteData(List<TopicPartition> partitions, PubSubMessage message) throws PubSubException {
        try {
            TopicPartition partition = getPartition(partitions, message);
            message.setMetadata(message.hasMetadata() ? new KafkaMetadata(message.getMetadata(), partition) :
                                new KafkaMetadata(partition));
        } catch (Exception e) {
            throw new PubSubException("Could not set route metadata.", e);
        }
    }

    /**
     * Given a {@link PubSubMessage} with routing data set by {@link #setRouteData(List, PubSubMessage)}, returns the
     * routing data or {@link TopicPartition} set within it.
     *
     * @param message The {@link PubSubMessage} with routing data.
     * @return The {@link TopicPartition} contained within.
     * @throws PubSubException if there were issues getting the routing data.
     */
    public static TopicPartition getRouteInfo(PubSubMessage message) throws PubSubException {
        try {
            KafkaMetadata metadata = (KafkaMetadata) message.getMetadata();
            return Objects.requireNonNull(metadata.getTopicPartition());
        } catch (Exception e) {
            throw new PubSubException("Invalid route information", e);
        }
    }
}
