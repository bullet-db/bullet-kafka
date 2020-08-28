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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUtils {
    public static void publishRandomMessages(Publisher publisher, int idCount, int messageCount) throws PubSubException {
        String randomMessage;
        String randomId;
        for (int i = 0; i < idCount; i++) {
            randomId = UUID.randomUUID().toString();
            for (int j = 0; j < messageCount; j++) {
                randomMessage = UUID.randomUUID().toString();
                publisher.send(randomId, randomMessage.getBytes(PubSubMessage.CHARSET));
            }
        }
    }

    public static TopicPartition getSendPartition(ProducerRecord<String, byte[]> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    public static TopicPartition getMetadataPartition(ProducerRecord<String, byte[]> record) {
        PubSubMessage message = SerializerDeserializer.fromBytes(record.value());
        return ((KafkaMetadata) message.getMetadata()).getTopicPartition();
    }

    public static String getMessageID(ProducerRecord<String, byte[]> record) {
        PubSubMessage message = SerializerDeserializer.fromBytes(record.value());
        return message.getId();
    }

    public static PubSubMessage getMessage(ProducerRecord<String, byte[]> record) {
        return SerializerDeserializer.fromBytes(record.value());
    }

    public static String getRandomString() {
        return UUID.randomUUID().toString();
    }

    public static KafkaProducer<String, byte[]> mockProducerTo(MessageStore messageStore) {
        KafkaProducer<String, byte[]> mockProducer = mock(KafkaProducer.class);
        when(mockProducer.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
                Object[] arguments = invocation.getArguments();
                messageStore.putRecord((ProducerRecord<String, byte[]>) arguments[0]);
                return null;
            }
        );
        return mockProducer;
    }

    public static <K, V> Map<K, Set<V>> groupTriples(List<MessageStore.Triple> messages,
                                                     Function<MessageStore.Triple, K> keyGen,
                                                     Function<MessageStore.Triple, V> valueGen) {
        Map<K, Set<V>> groupedData = new HashMap<>();
        for (MessageStore.Triple message : messages) {
            K key = keyGen.apply(message);
            Set<V> messageSet = groupedData.getOrDefault(key, new HashSet<>());
            messageSet.add(valueGen.apply(message));
            groupedData.put(key, messageSet);
        }
        return groupedData;
    }
}
