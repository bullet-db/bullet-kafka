/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Publisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaQueryPublisherTest {
    private static final int NUM_PARTITIONS = 10;
    private static final int NUM_IDS = 100;

    private static MessageStore messageStore = new MessageStore();
    private static KafkaProducer<String, byte[]> mockProducer = TestUtils.mockProducerTo(messageStore);
    private static List<TopicPartition> requestPartitionList = IntStream.range(0, NUM_PARTITIONS)
                                                                        .mapToObj(x -> new TopicPartition("topic", x))
                                                                        .collect(Collectors.toList());
    private static List<TopicPartition> responsePartitionList = new ArrayList<>(requestPartitionList);
    private static Publisher publisher = new KafkaQueryPublisher(mockProducer, requestPartitionList, responsePartitionList, "topic", true);

    @BeforeMethod
    public void setup() throws PubSubException {
        // Clear the message store and publish some random messages.
        messageStore.clear();
        TestUtils.publishRandomMessages(publisher, NUM_IDS, 10);
    }

    @Test
    public void testUniqueRequestPartitionPerId() {
        for (Set<TopicPartition> value : messageStore.groupSendPartitionById().values()) {
            Assert.assertTrue(value.size() <= 1);
        }
    }

    @Test
    public void testUniqueResponsePartitionPerId() {
        for (Set<TopicPartition> value : messageStore.groupReceivePartitionById().values()) {
            Assert.assertTrue(value.size() <= 1);
        }
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testSendMessageWithBadMetadata() throws Exception {
        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.doThrow(new RuntimeException("Testing")).when(metadata).getContent();
        publisher.send(new PubSubMessage("foo", "bar", metadata));
    }

    @Test
    public void testClosesPublisher()  throws Exception {
        publisher.close();
        Mockito.verify(mockProducer).close();
    }

    @Test
    public void testConstructorInjectsArgs() {
        KafkaQueryPublisher kafkaQueryPublisher = (KafkaQueryPublisher) publisher;
        Assert.assertEquals(kafkaQueryPublisher.getReceivePartitions(), responsePartitionList);
        Assert.assertEquals(kafkaQueryPublisher.getWritePartitions(), requestPartitionList);
        Assert.assertEquals(kafkaQueryPublisher.getProducer(), mockProducer);
        Assert.assertEquals(kafkaQueryPublisher.getQueryTopic(), "topic");
        Assert.assertTrue(kafkaQueryPublisher.isPartitionRoutingEnabled());
    }

    @Test
    public void testPartitionRoutingDisabled() throws PubSubException {
        messageStore.clear();

        Publisher publisher = new KafkaQueryPublisher(mockProducer, requestPartitionList, responsePartitionList, "topic", false);
        PubSubMessage message = publisher.send(new PubSubMessage("foo", "bar", new Metadata()));
        Map<String, Set<TopicPartition>> sentMessages = messageStore.groupSendPartitionById();

        Assert.assertFalse(message.getMetadata() instanceof KafkaMetadata);
        Assert.assertEquals(sentMessages.size(), 1);
        Assert.assertTrue(sentMessages.keySet().contains("foo"));
        Assert.assertTrue(sentMessages.get("foo").contains(new TopicPartition("topic", -1)));
    }
}
