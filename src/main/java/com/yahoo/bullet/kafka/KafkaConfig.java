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
    // Common Properties
    public static final String BOOTSTRAP_SERVERS = "bullet.pubsub.kafka.bootstrap.servers";
    public static final String REQUEST_PARTITIONS = "bullet.pubsub.kafka.request.partitions";
    public static final String RESPONSE_PARTITIONS = "bullet.pubsub.kafka.response.partitions";
    public static final String REQUEST_TOPIC_NAME = "bullet.pubsub.kafka.request.topic.name";
    public static final String RESPONSE_TOPIC_NAME = "bullet.pubsub.kafka.response.topic.name";

    // Producer Specific Properties
    public static final String ACKS = "bullet.pubsub.kafka.acks";
    public static final String RETRIES = "bullet.pubsub.kafka.retries";
    public static final String BATCH_SIZE = "bullet.pubsub.kafka.batch.size";
    public static final String LINGER = "bullet.pubsub.kafka.linger.ms";
    public static final String BUFFER_MEMORY = "bullet.pubsub.kafka.buffer.memory";
    public static final String REQUEST_TIMEOUT = "bullet.pubsub.kafka.request.timeout.ms";
    public static final String KEY_SERIALIZER = "bullet.pubsub.kafka.key.serializer";
    public static final String VALUE_SERIALIZER = "bullet.pubsub.kafka.value.serializer";
    public static final String MAX_BLOCK_MS = "bullet.pubsub.kafka.max.block.ms";

    // Required Kafka Producer Properties
    public static final Set<String> KAFKA_PRODUCER_PROPERTIES = new HashSet<>(asList(BOOTSTRAP_SERVERS, ACKS, RETRIES,
                                                                                     BATCH_SIZE, LINGER, BUFFER_MEMORY,
                                                                                     REQUEST_TIMEOUT, KEY_SERIALIZER,
                                                                                     VALUE_SERIALIZER, MAX_BLOCK_MS));

    // Consumer Specific Properties
    public static final String MAX_UNACKED_MESSAGES = "bullet.pubsub.kafka.max.unacked.messages";
    public static final String HEARTBEAT_INTERVAL_MS = "bullet.pubsub.kafka.heartbeat.interval.ms";
    public static final String AUTO_COMMIT_INTERVAL_MS = "bullet.pubsub.kafka.auto.commit.interval.ms";
    public static final String SESSION_TIMEOUT_MS = "bullet.pubsub.kafka.session.timeout.ms";
    public static final String MAX_POLL_RECORDS = "bullet.pubsub.kafka.max.poll.records";
    public static final String KEY_DESERIALIZER = "bullet.pubsub.kafka.key.deserializer";
    public static final String VALUE_DESERIALIZER = "bullet.pubsub.kafka.value.deserializer";
    public static final String ENABLE_AUTO_COMMIT = "bullet.pubsub.kafka.enable.auto.commit";
    public static final String FETCH_MAX_WAIT_MS = "bullet.pubsub.kafka.fetch.max.wait.ms";
    public static final String GROUP_ID = "bullet.pubsub.kafka.group.id";

    // Required Kafka Consumer Properties
    public static final Set<String> KAFKA_CONSUMER_PROPERTIES = new HashSet<>(asList(BOOTSTRAP_SERVERS, GROUP_ID,
                                                                                     AUTO_COMMIT_INTERVAL_MS,
                                                                                     MAX_POLL_RECORDS,
                                                                                     ENABLE_AUTO_COMMIT,
                                                                                     KEY_DESERIALIZER,
                                                                                     VALUE_DESERIALIZER,
                                                                                     HEARTBEAT_INTERVAL_MS,
                                                                                     SESSION_TIMEOUT_MS,
                                                                                     FETCH_MAX_WAIT_MS));

    public static final String DEFAULT_KAFKA_CONFIGURATION = "bullet_kafka_defaults.yaml";

    public static final String KAFKA_NAMESPACE = "bullet.pubsub.kafka";

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
