/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.Metadata;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;

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
}
