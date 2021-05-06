/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.kafka.KafkaConfig.CONSUMER_NAMESPACE;
import static com.yahoo.bullet.kafka.KafkaConfig.KAFKA_NAMESPACE;
import static com.yahoo.bullet.kafka.KafkaConfig.PRODUCER_NAMESPACE;

@Slf4j
public class KafkaPubSub extends PubSub {
    private List<TopicPartition> queryPartitions;
    private List<TopicPartition> responsePartitions;
    private String queryTopicName;
    private String responseTopicName;
    private String topic;
    private boolean partitionRoutingEnabled;
    private List<TopicPartition> partitions;
    private Map<String, Object> producerProperties;
    private Map<String, Object> consumerProperties;

    /**
     * Creates a KafkaPubSub from a {@link BulletConfig}.
     *
     * @param pubSubConfig The {@link BulletConfig} to load settings from.
     * @throws PubSubException if Kafka defaults cannot be loaded or Kafka broker cannot be reached.
     */
    public KafkaPubSub(BulletConfig pubSubConfig) throws PubSubException {
        super(pubSubConfig);
        // Copy settings from pubSubConfig.
        config = new KafkaConfig(pubSubConfig);
        initialize();
    }

    @Override
    public void switchContext(Context context, BulletConfig config) throws PubSubException {
        super.switchContext(context, config);
        initialize();
    }

    private void initialize() {
        queryTopicName = config.getAs(KafkaConfig.REQUEST_TOPIC_NAME, String.class);
        responseTopicName  = config.getAs(KafkaConfig.RESPONSE_TOPIC_NAME, String.class);
        topic = (context == Context.QUERY_PROCESSING) ? queryTopicName : responseTopicName;
        partitionRoutingEnabled = config.getAs(KafkaConfig.PARTITION_ROUTING_ENABLE, Boolean.class);

        queryPartitions = parsePartitionsFor(queryTopicName, KafkaConfig.REQUEST_PARTITIONS);
        responsePartitions = parsePartitionsFor(responseTopicName, KafkaConfig.RESPONSE_PARTITIONS);
        partitions = (context == Context.QUERY_PROCESSING) ? queryPartitions : responsePartitions;

        Map<String, Object> commonProperties = config.getAllWithPrefix(Optional.of(KafkaConfig.COMMON_PROPERTIES), KAFKA_NAMESPACE, true);
        producerProperties = config.getAllWithPrefix(Optional.empty(), PRODUCER_NAMESPACE, true);
        producerProperties.putAll(commonProperties);
        log.info("Producer properties:\n{}", producerProperties);
        consumerProperties = config.getAllWithPrefix(Optional.empty(), CONSUMER_NAMESPACE, true);
        consumerProperties.putAll(commonProperties);
        log.info("Consumer properties:\n{}", consumerProperties);
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProperties);

        if (context == Context.QUERY_PROCESSING) {
            // We don't provide topic partitions here since they should be in the message metadata when partition routing
            // is enabled. When partition routing is not enabled, the publisher uses the given topic name instead.
            return new KafkaResponsePublisher(producer, responseTopicName, partitionRoutingEnabled);
        }

        List<TopicPartition> to = (queryPartitions == null) ? getAllPartitions(getDummyProducer(), queryTopicName) : queryPartitions;
        List<TopicPartition> from = (responsePartitions == null) ? getAllPartitions(getDummyProducer(), responseTopicName) : responsePartitions;

        return new KafkaQueryPublisher(producer, to, from, partitionRoutingEnabled);
    }

    @Override
    public List<Publisher> getPublishers(int n) throws PubSubException {
        // Kafka Publishers are thread safe and can be reused
        return Collections.nCopies(n, getPublisher());
    }

    @Override
    public Subscriber getSubscriber() throws PubSubException {
        return getSubscriber(partitions, topic);
    }

    /**
     * Attempts to allocate available partitions to n {@link Subscriber} objects. If an exact allocation is not
     * possible, it returns less than n Subscribers with partitions allocated as evenly as possible among them.
     *
     * @param n The number of Subscribers requested.
     * @return A {@link List} containing the requested Subscribers.
     * @throws PubSubException if unable to create Subscribers.
     */
    @Override
    public List<Subscriber> getSubscribers(int n) throws PubSubException {
        List<Subscriber> subscribers = new ArrayList<>();
        if (partitions == null) {
            for (int i = 0; i < n; ++i) {
                subscribers.add(getSubscriber());
            }
            return subscribers;
        }
        // Try to divide the partitions equally
        int totalPartitions = partitions.size();
        int partitionsPerSubscriber = (int) Math.ceil(totalPartitions / ((double) n));
        int start = 0;
        while (start < totalPartitions) {
            int end = start + partitionsPerSubscriber;
            subscribers.add(getSubscriber(partitions.subList(start, Math.min(end, totalPartitions)), topic));
            start = end;
        }
        return subscribers;
    }

    /**
     * Safely reads the partition list from the field in the YAML file. If no such setting exists, returns null to
     * signify default to all partitions in the topic.
     *
     * @param topicName The name of the topic to get partitions for.
     * @param fieldName The key corresponding to the partition list in the YAML file.
     * @return {@link List} of {@link TopicPartition} values assigned in {@link KafkaConfig}.
     */
    private List<TopicPartition> parsePartitionsFor(String topicName, String fieldName) {
        if (config.get(fieldName) == null) {
            return null;
        }
        List<TopicPartition> partitionList = new ArrayList<>();
        List<Number> partitionObjectList = config.getAs(fieldName, List.class);
        for (Number partition : partitionObjectList) {
            partitionList.add(new TopicPartition(topicName, partition.intValue()));
        }
        return partitionList;
    }

    /**
     * Get all partitions for a given topic.
     *
     * @param topicName The topic to get partitions for.
     * @return {@link List} of {@link TopicPartition} values corresponding to the topic.
     */
    List<TopicPartition> getAllPartitions(KafkaProducer<String, byte[]> dummy, String topicName) {
        List<TopicPartition> partitions = dummy.partitionsFor(topicName)
                                               .stream().map(i -> new TopicPartition(i.topic(), i.partition()))
                                               .collect(Collectors.toList());
        dummy.close();
        return partitions;
    }

    /**
     * Get a Subscriber that reads from the given partitions. If partitions is null, the Subscriber reads from the topic
     * corresponding to topicName.
     *
     * @param partitions The list of partitions to read from.
     * @param topicName The topic to subscribe to if partitions are not given.
     * @return The Subscriber reading from the appropriate topic/partitions.
     */
    private Subscriber getSubscriber(List<TopicPartition> partitions, String topicName) throws PubSubException {
        // Get the PubSub Consumer specific properties
        Number maxUnackedMessages = config.getAs(KafkaConfig.MAX_UNCOMMITTED_MESSAGES, Number.class);
        Number rateLimitMaxMessages = config.getAs(KafkaConfig.RATE_LIMIT_MAX_MESSAGES, Number.class);
        Number rateLimitIntervalMS = config.getAs(KafkaConfig.RATE_LIMIT_INTERVAL_MS, Number.class);
        boolean rateLimitEnable = config.getAs(KafkaConfig.RATE_LIMIT_ENABLE, Boolean.class);

        // Is autocommit on
        boolean enableAutoCommit = Boolean.parseBoolean(config.getAs(KafkaConfig.ENABLE_AUTO_COMMIT, String.class));

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties);
        // Subscribe to the topic if partitions are not set in the config.
        if (partitions == null) {
            consumer.subscribe(Collections.singleton(topicName));
        } else {
            consumer.assign(partitions);
        }
        if (rateLimitEnable) {
            return new KafkaSubscriber(consumer, maxUnackedMessages.intValue(), rateLimitMaxMessages.intValue(),
                                       rateLimitIntervalMS.longValue(), !enableAutoCommit);
        }
        return new KafkaSubscriber(consumer, maxUnackedMessages.intValue(), !enableAutoCommit);
    }

    private KafkaProducer<String, byte[]> getDummyProducer() {
        return new KafkaProducer<>(producerProperties);
    }
}
