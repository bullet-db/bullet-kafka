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

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class KafkaResponsePublisherTest {
    @Test
    public void testSendPartition() throws PubSubException {
        MessageStore messageStore = new MessageStore();
        KafkaProducer<String, byte[]> mockProducer = TestUtils.mockProducerTo(messageStore);
        TopicPartition randomTopicPartition = new TopicPartition(TestUtils.getRandomString(), 0);
        String randomID = TestUtils.getRandomString();
        Publisher publisher = new KafkaResponsePublisher(mockProducer, "topic", true);
        publisher.send(new PubSubMessage(randomID, "", new KafkaMetadata(randomTopicPartition)));
        Map<String, Set<TopicPartition>> sentMessages = messageStore.groupSendPartitionById();

        Assert.assertEquals(sentMessages.size(), 1);
        Assert.assertTrue(sentMessages.keySet().contains(randomID));
        Assert.assertTrue(sentMessages.get(randomID).contains(randomTopicPartition));
    }

    @Test
    public void testSendPartitionPartitionRoutingDisabled() throws PubSubException {
        MessageStore messageStore = new MessageStore();
        KafkaProducer<String, byte[]> mockProducer = TestUtils.mockProducerTo(messageStore);
        TopicPartition randomTopicPartition = new TopicPartition("topic", -1);
        String randomID = TestUtils.getRandomString();
        Publisher publisher = new KafkaResponsePublisher(mockProducer, "topic", false);
        publisher.send(new PubSubMessage(randomID, "", new Metadata()));
        Map<String, Set<TopicPartition>> sentMessages = messageStore.groupSendPartitionById();

        Assert.assertEquals(sentMessages.size(), 1);
        Assert.assertTrue(sentMessages.keySet().contains(randomID));
        Assert.assertTrue(sentMessages.get(randomID).contains(randomTopicPartition));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testInvalidRouteInformation() throws PubSubException {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, "topic", true);
        publisher.send(new PubSubMessage("", "", new Metadata(null, "")));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testNoRouteInformation() throws PubSubException {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, "topic", true);
        publisher.send(new PubSubMessage("", "", new Metadata(null, null)));
    }

    @Test
    public void testClose() throws Exception {
        KafkaProducer<String, byte[]> mockProducer = (KafkaProducer<String, byte[]>) mock(KafkaProducer.class);
        Publisher publisher = new KafkaResponsePublisher(mockProducer, "topic", true);
        publisher.close();
        verify(mockProducer).close();
    }
}
