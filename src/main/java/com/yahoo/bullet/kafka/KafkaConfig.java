/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Config;
import com.yahoo.bullet.common.Validator;

public class KafkaConfig extends BulletConfig {
    public static final String KAFKA_NAMESPACE = "bullet.pubsub.kafka" + DELIMITER;

    // Common Kafka properties
    public static final String BOOTSTRAP_SERVERS = KAFKA_NAMESPACE + "bootstrap.servers";

    public static final String PRODUCER_NAMESPACE = KAFKA_NAMESPACE + "producer" + DELIMITER;
    // Producer specific properties
    public static final String KEY_SERIALIZER = PRODUCER_NAMESPACE + "key.serializer";
    public static final String VALUE_SERIALIZER = PRODUCER_NAMESPACE + "value.serializer";

    public static final String CONSUMER_NAMESPACE = KAFKA_NAMESPACE + "consumer" + DELIMITER;
    // Consumer specific properties
    public static final String GROUP_ID = CONSUMER_NAMESPACE + "group.id";
    public static final String ENABLE_AUTO_COMMIT = CONSUMER_NAMESPACE + "enable.auto.commit";
    public static final String KEY_DESERIALIZER = CONSUMER_NAMESPACE + "key.deserializer";
    public static final String VALUE_DESERIALIZER = CONSUMER_NAMESPACE + "value.deserializer";

    // Kafka PubSub properties
    public static final String REQUEST_PARTITIONS = KAFKA_NAMESPACE + "request.partitions";
    public static final String RESPONSE_PARTITIONS = KAFKA_NAMESPACE + "response.partitions";
    public static final String REQUEST_TOPIC_NAME = KAFKA_NAMESPACE + "request.topic.name";
    public static final String RESPONSE_TOPIC_NAME = KAFKA_NAMESPACE + "response.topic.name";

    // Kafka PubSub Subscriber properties
    public static final String MAX_UNCOMMITTED_MESSAGES = KAFKA_NAMESPACE + "subscriber.max.uncommitted.messages";

    // Defaults
    public static final String DEFAULT_KAFKA_CONFIGURATION = "bullet_kafka_defaults.yaml";
    public static final boolean DEFAULT_ENABLE_AUTO_COMMIT = true;

    private static final Validator VALIDATOR = BulletConfig.getValidator();

    static {
        VALIDATOR.define(BOOTSTRAP_SERVERS)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(REQUEST_TOPIC_NAME)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(RESPONSE_TOPIC_NAME)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(REQUEST_PARTITIONS)
                 .checkIf(Validator.isListOfType(Long.class))
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(RESPONSE_PARTITIONS)
                 .checkIf(Validator.isListOfType(Long.class))
                 .unless(Validator::isNull)
                 .orFail();
        VALIDATOR.define(KEY_SERIALIZER)
                 .checkIf(Validator::isClassName)
                 .orFail();
        VALIDATOR.define(VALUE_SERIALIZER)
                 .checkIf(Validator::isClassName)
                 .orFail();
        VALIDATOR.define(KEY_DESERIALIZER)
                 .checkIf(Validator::isClassName)
                 .orFail();
        VALIDATOR.define(VALUE_DESERIALIZER)
                 .checkIf(Validator::isClassName)
                 .orFail();
        VALIDATOR.define(GROUP_ID)
                 .checkIf(Validator::isString)
                 .orFail();
        VALIDATOR.define(ENABLE_AUTO_COMMIT)
                 .checkIf(Validator::isBoolean)
                 .defaultTo(DEFAULT_ENABLE_AUTO_COMMIT);
    }

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
        VALIDATOR.validate(this);
    }
}
