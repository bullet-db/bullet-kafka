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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.kafka.KafkaMetadata.getPartition;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class KafkaResponsePublisherTest {
    private static final int NUM_PARTITIONS = 10;
    private static List<TopicPartition> responsePartitionList = IntStream.range(0, NUM_PARTITIONS)
                                                                         .mapToObj(x -> new TopicPartition("topic", x))
                                                                         .collect(Collectors.toList());

    @Test
    public void testSendPartition() throws PubSubException {
        MessageStore messageStore = new MessageStore();
        KafkaProducer<String, byte[]> mockProducer = TestUtils.mockProducerTo(messageStore);
        TopicPartition topicPartition = new TopicPartition("topic", 0);
        String randomID = TestUtils.getRandomString();
        Publisher publisher = new KafkaResponsePublisher(mockProducer, responsePartitionList, true);
        publisher.send(new PubSubMessage(randomID, "", new KafkaMetadata(topicPartition)));

        Map<String, Set<TopicPartition>> sentMessages = messageStore.groupSendPartitionById();
        Assert.assertEquals(sentMessages.size(), 1);
        Assert.assertTrue(sentMessages.containsKey(randomID));
        Assert.assertTrue(sentMessages.get(randomID).contains(topicPartition));
    }

    @Test
    public void testSendPartitionPartitionRoutingDisabled() throws PubSubException {
        MessageStore messageStore = new MessageStore();
        KafkaProducer<String, byte[]> mockProducer = TestUtils.mockProducerTo(messageStore);
        String randomID = TestUtils.getRandomString();
        Publisher publisher = new KafkaResponsePublisher(mockProducer, responsePartitionList, false);
        PubSubMessage message = publisher.send(new PubSubMessage(randomID, "", new Metadata()));

        Map<String, Set<TopicPartition>> sentMessages = messageStore.groupSendPartitionById();
        Assert.assertEquals(sentMessages.size(), 1);
        Assert.assertTrue(sentMessages.containsKey(randomID));
        TopicPartition expected = new TopicPartition("topic", getPartition(responsePartitionList, message).partition());
        Assert.assertTrue(sentMessages.get(randomID).contains(expected));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testInvalidRouteInformation() throws PubSubException {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, responsePartitionList, true);
        publisher.send(new PubSubMessage("", "", new Metadata(null, "")));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testNoRouteInformation() throws PubSubException {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, responsePartitionList, true);
        publisher.send(new PubSubMessage("", "", new Metadata(null, null)));
    }

    @Test
    public void testClose() throws Exception {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, responsePartitionList, true);
        publisher.close();
        verify(mockProducer).close();
    }
}
