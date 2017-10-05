/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Config;

import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

public class KafkaConfig extends BulletConfig {
    public static final String TRUE = "true";

    public static final String KAFKA_NAMESPACE = "bullet.pubsub.kafka" + DELIMITER;

    // Common Kafka properties
    public static final String BOOTSTRAP_SERVERS = KAFKA_NAMESPACE + "bootstrap.servers";
    public static final String CONNECTIONS_MAX_IDLE_MS = KAFKA_NAMESPACE + "connections.max.idle.ms";

    // Required common Kafka properties for producers and consumers
    public static final Set<String> KAFKA_PROPERTIES = new HashSet<>(asList(BOOTSTRAP_SERVERS, CONNECTIONS_MAX_IDLE_MS));

    // Producer specific properties
    public static final String PRODUCER_NAMESPACE = KAFKA_NAMESPACE + "producer" + DELIMITER;

    public static final String ACKS = PRODUCER_NAMESPACE + "acks";
    public static final String RETRIES = PRODUCER_NAMESPACE + "retries";
    public static final String BATCH_SIZE = PRODUCER_NAMESPACE + "batch.size";
    public static final String LINGER = PRODUCER_NAMESPACE + "linger.ms";
    public static final String BUFFER_MEMORY = PRODUCER_NAMESPACE + "buffer.memory";
    public static final String REQUEST_TIMEOUT = PRODUCER_NAMESPACE + "request.timeout.ms";
    public static final String KEY_SERIALIZER = PRODUCER_NAMESPACE + "key.serializer";
    public static final String VALUE_SERIALIZER = PRODUCER_NAMESPACE + "value.serializer";
    public static final String MAX_BLOCK_MS = PRODUCER_NAMESPACE + "max.block.ms";

    // Required Kafka producer properties
    public static final Set<String> KAFKA_PRODUCER_PROPERTIES = new HashSet<>(asList(ACKS, RETRIES,
                                                                                     BATCH_SIZE, LINGER, BUFFER_MEMORY,
                                                                                     REQUEST_TIMEOUT, KEY_SERIALIZER,
                                                                                     VALUE_SERIALIZER, MAX_BLOCK_MS));
    static {
        KAFKA_PRODUCER_PROPERTIES.addAll(KAFKA_PROPERTIES);
    }

    // Consumer specific properties
    public static final String CONSUMER_NAMESPACE = KAFKA_NAMESPACE + "consumer" + DELIMITER;

    public static final String GROUP_ID = CONSUMER_NAMESPACE + "group.id";
    public static final String HEARTBEAT_INTERVAL_MS = CONSUMER_NAMESPACE + "heartbeat.interval.ms";
    public static final String ENABLE_AUTO_COMMIT = CONSUMER_NAMESPACE + "enable.auto.commit";
    public static final String AUTO_COMMIT_INTERVAL_MS = CONSUMER_NAMESPACE + "auto.commit.interval.ms";
    public static final String SESSION_TIMEOUT_MS = CONSUMER_NAMESPACE + "session.timeout.ms";
    public static final String MAX_POLL_RECORDS = CONSUMER_NAMESPACE + "max.poll.records";
    public static final String KEY_DESERIALIZER = CONSUMER_NAMESPACE + "key.deserializer";
    public static final String VALUE_DESERIALIZER = CONSUMER_NAMESPACE + "value.deserializer";
    public static final String FETCH_MAX_WAIT_MS = CONSUMER_NAMESPACE + "fetch.max.wait.ms";

    // Required Kafka consumer properties
    public static final Set<String> KAFKA_CONSUMER_PROPERTIES = new HashSet<>(asList(GROUP_ID,
                                                                                     AUTO_COMMIT_INTERVAL_MS,
                                                                                     MAX_POLL_RECORDS,
                                                                                     ENABLE_AUTO_COMMIT,
                                                                                     KEY_DESERIALIZER,
                                                                                     VALUE_DESERIALIZER,
                                                                                     HEARTBEAT_INTERVAL_MS,
                                                                                     SESSION_TIMEOUT_MS,
                                                                                     FETCH_MAX_WAIT_MS));
    static {
        KAFKA_CONSUMER_PROPERTIES.addAll(KAFKA_PROPERTIES);
    }

    // Kafka PubSub properties
    public static final String REQUEST_PARTITIONS = KAFKA_NAMESPACE + "request.partitions";
    public static final String RESPONSE_PARTITIONS = KAFKA_NAMESPACE + "response.partitions";
    public static final String REQUEST_TOPIC_NAME = KAFKA_NAMESPACE + "request.topic.name";
    public static final String RESPONSE_TOPIC_NAME = KAFKA_NAMESPACE + "response.topic.name";
    // Kafka Consumer PubSub properties
    public static final String MAX_UNCOMMITTED_MESSAGES = CONSUMER_NAMESPACE + "max.uncommitted.messages";

    public static final String DEFAULT_KAFKA_CONFIGURATION = "bullet_kafka_defaults.yaml";

    /**
     * Creates a KafkaConfig by reading in a file.
     *
     * @param file The file to read in to create the KafkaConfig.
     */
    public KafkaConfig(String file) {
        this(new Config(file));
    }

    /**
     * Creates a KafkaConfig from a Config.
     *
     * @param config The {@link Config} to copy settings from.
     */
    public KafkaConfig(Config config) {
        // Load default Kafka settings. Merge additional settings in Config
        super(DEFAULT_KAFKA_CONFIGURATION);
        merge(config);
    }
}
