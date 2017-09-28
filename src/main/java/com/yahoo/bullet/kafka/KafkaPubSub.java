/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.pubsub.PubSub;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.Publisher;
import com.yahoo.bullet.pubsub.Subscriber;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.kafka.KafkaConfig.KAFKA_CONSUMER_PROPERTIES;
import static com.yahoo.bullet.kafka.KafkaConfig.KAFKA_PRODUCER_PROPERTIES;

public class KafkaPubSub extends PubSub {
    private List<TopicPartition> queryPartitions;
    private List<TopicPartition> responsePartitions;
    private String queryTopicName;
    private String responseTopicName;
    private String topic;
    private List<TopicPartition> partitions;

    public static final String SETTING_PREFIX = KafkaConfig.KAFKA_NAMESPACE + KafkaConfig.DELIMITER;

    /**
     * Creates a KafkaPubSub from a {@link BulletConfig}.
     *
     * @param pubSubConfig The {@link BulletConfig} to load settings from.
     * @throws PubSubException if Kafka defaults cannot be loaded or Kafka broker cannot be reached.
     */
    public KafkaPubSub(BulletConfig pubSubConfig) throws PubSubException {
        super(pubSubConfig);
        // Copy settings from pubSubConfig.
        try {
            config = new KafkaConfig(pubSubConfig);
        } catch (IOException e) {
            throw new PubSubException("Could not create KafkaConfig", e);
        }

        queryTopicName = getRequiredConfig(String.class, KafkaConfig.REQUEST_TOPIC_NAME);
        responseTopicName  = getRequiredConfig(String.class, KafkaConfig.RESPONSE_TOPIC_NAME);
        topic = (context == Context.QUERY_PROCESSING) ? queryTopicName : responseTopicName;

        queryPartitions = parsePartitionsFor(queryTopicName, KafkaConfig.REQUEST_PARTITIONS);
        responsePartitions = parsePartitionsFor(responseTopicName, KafkaConfig.RESPONSE_PARTITIONS);
        partitions = (context == Context.QUERY_PROCESSING) ? queryPartitions : responsePartitions;
    }

    @Override
    public Publisher getPublisher() throws PubSubException {
        Map<String, Object> properties = config.getAllWithPrefix(Optional.of(KAFKA_PRODUCER_PROPERTIES), SETTING_PREFIX, true);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        if (context == Context.QUERY_PROCESSING) {
            // We don't need to provide topic-partitions here since they should be in the message metadata
            return new KafkaResponsePublisher(producer);
        }

        List<TopicPartition> to = (queryPartitions == null) ? getAllPartitions(getDummyProducer(), queryTopicName) : queryPartitions;
        List<TopicPartition> from = (responsePartitions == null) ? getAllPartitions(getDummyProducer(), responseTopicName) : responsePartitions;

        return new KafkaQueryPublisher(producer, to, from);
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
     * @throws PubSubException if the setting for partitions is malformed.
     */
    private List<TopicPartition> parsePartitionsFor(String topicName, String fieldName) throws PubSubException {
        if (config.get(fieldName) == null) {
            return null;
        }
        List<TopicPartition> partitionList = new ArrayList<>();
        List partitionObjectList = getRequiredConfig(List.class, fieldName);
        for (Object partition : partitionObjectList) {
            if (!(partition instanceof Long)) {
                throw new PubSubException(fieldName + "must be a list of integers.");
            }
            partitionList.add(new TopicPartition(topicName, ((Long) partition).intValue()));
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
        Map<String, Object> properties = config.getAllWithPrefix(Optional.of(KAFKA_CONSUMER_PROPERTIES), SETTING_PREFIX, true);
        Long maxUnackedMessages = getRequiredConfig(Long.class, KafkaConfig.MAX_UNACKED_MESSAGES);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        // Subscribe to the topic if partitions are not set in the config.
        if (partitions == null) {
            consumer.subscribe(Collections.singleton(topicName));
        } else {
            consumer.assign(partitions);
        }
        return new KafkaSubscriber(consumer, maxUnackedMessages.intValue());
    }

    private KafkaProducer<String, byte[]> getDummyProducer() {
        Map<String, Object> properties = config.getAllWithPrefix(Optional.of(KAFKA_PRODUCER_PROPERTIES), SETTING_PREFIX, true);
        return new KafkaProducer<>(properties);
    }
}
