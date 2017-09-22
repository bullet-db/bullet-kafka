/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.PubSubMessage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class MessageStore {
    @AllArgsConstructor @Getter @Setter
    public class Triple {
        private TopicPartition sendPartition;
        private TopicPartition receivePartition;
        private PubSubMessage message;
    }

    private List<Triple> messages = new ArrayList<>();
    private Triple triple;

    public void putRecord(ProducerRecord<String, byte[]> record) {
        triple = new Triple(TestUtils.getSendPartition(record), TestUtils.getMetadataPartition(record), TestUtils.getMessage(record));
        messages.add(triple);
    }

    public List<PubSubMessage> getMessages() {
        return messages.stream().map(Triple::getMessage).collect(Collectors.toList());
    }

    public Map<String, Set<TopicPartition>> groupSendPartitionById() {
        return TestUtils.groupTriples(messages, x -> x.getMessage().getId(), Triple::getSendPartition);
    }

    public Map<String, Set<TopicPartition>> groupReceivePartitionById() {
        return TestUtils.groupTriples(messages, x -> x.getMessage().getId(), Triple::getReceivePartition);
    }

    public Map<TopicPartition, Set<String>> groupIdBySendPartition() {
        return TestUtils.groupTriples(messages, Triple::getSendPartition, x -> x.getMessage().getId());
    }

    public Map<TopicPartition, Set<String>> groupIdByReceivePartition() {
        return TestUtils.groupTriples(messages, Triple::getReceivePartition, x->x.getMessage().getId());
    }

    public void clear() {
        messages.clear();
    }
}
