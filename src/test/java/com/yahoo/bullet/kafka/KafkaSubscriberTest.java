/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.pubsub.PubSubException;
import com.yahoo.bullet.pubsub.PubSubMessage;
import com.yahoo.bullet.pubsub.Subscriber;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.UUID;

import static com.yahoo.bullet.kafka.TestUtils.makeConsumerRecords;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class KafkaSubscriberTest {
    private KafkaConsumer<String, byte[]> makeMockConsumer(String randomID, String randomMessage) {
        KafkaConsumer<String, byte[]> consumer = (KafkaConsumer<String, byte[]>) mock(KafkaConsumer.class);
        ConsumerRecords<String, byte[]> records = makeConsumerRecords(randomID, new PubSubMessage(randomID, randomMessage, (Metadata) null));
        when(consumer.poll(any())).thenReturn(records).thenReturn(new ConsumerRecords<>(new HashMap<>()));
        return consumer;
    }

    private boolean getAndCheck(String randomMessage, String randomID, KafkaSubscriber subscriber) throws PubSubException {
        PubSubMessage message = subscriber.receive();
        Assert.assertNotNull(message);
        subscriber.commit(message.getId());
        // Test if correct message is received.
        boolean result = (message.getContentAsString().equals(randomMessage) && message.getId().equals(randomID));
        // Test if next message is null.
        message = subscriber.receive();
        return result && (message == null);
    }

    @Test
    public void testReceiveCommit() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();

        KafkaConsumer<String, byte[]> consumer = makeMockConsumer(randomID, randomMessage);
        KafkaSubscriber subscriber = new KafkaSubscriber(consumer, 50);
        Assert.assertTrue(getAndCheck(randomMessage, randomID, subscriber));
    }

    @Test
    public void testMessageFail() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();

        KafkaConsumer<String, byte[]> consumer = makeMockConsumer(randomID, randomMessage);
        KafkaSubscriber subscriber = new KafkaSubscriber(consumer, 50);
        PubSubMessage message = subscriber.receive();
        Assert.assertNotNull(message);
        subscriber.fail(message.getId());
        Assert.assertTrue(getAndCheck(randomMessage, randomID, subscriber));
    }

    @Test
    public void testMaxUnackedMessages() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();

        KafkaConsumer<String, byte[]> consumer = makeMockConsumer(randomID, randomMessage);
        KafkaSubscriber subscriber = new KafkaSubscriber(consumer, 1);
        // Multiple receives without a commit.
        Assert.assertNotNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());
    }

    @Test
    public void testCommitWhenAbsent() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();

        KafkaSubscriber subscriber = new KafkaSubscriber(makeMockConsumer(randomID, randomMessage), 10);
        // Make spurious commit.
        subscriber.commit(UUID.randomUUID().toString());
        Assert.assertTrue(getAndCheck(randomMessage, randomID, subscriber));
    }

    @Test
    public void testFailWhenAbsent() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();

        KafkaSubscriber subscriber = new KafkaSubscriber(makeMockConsumer(randomID, randomMessage), 10);
        // Make spurious fail.
        subscriber.fail(UUID.randomUUID().toString());
        Assert.assertTrue(getAndCheck(randomMessage, randomID, subscriber));
    }

    @Test(expectedExceptions = PubSubException.class)
    public void testKafkaError() throws PubSubException {
        KafkaConsumer<String, byte[]> consumer = (KafkaConsumer<String, byte[]>) mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenThrow(new KafkaException());
        Subscriber subscriber = new KafkaSubscriber(consumer, 100);
        subscriber.receive();
    }

    @Test
    public void testClose() throws Exception {
        KafkaConsumer<String, byte[]> consumer = (KafkaConsumer<String, byte[]>) mock(KafkaConsumer.class);
        Subscriber subscriber = new KafkaSubscriber(consumer, 100);
        subscriber.close();
        verify(consumer).close();
    }

    @Test
    public void testConstructorInjectsConsumer() {
        KafkaConsumer<String, byte[]> consumer = (KafkaConsumer<String, byte[]>) mock(KafkaConsumer.class);
        KafkaSubscriber subscriber = new KafkaSubscriber(consumer, 100);
        Assert.assertEquals(subscriber.getConsumer(), consumer);
    }

    @Test
    public void testManualCommitting() throws PubSubException {
        String randomMessage = UUID.randomUUID().toString();
        String randomID = UUID.randomUUID().toString();
        KafkaConsumer<String, byte[]> consumer = makeMockConsumer(randomID, randomMessage);
        KafkaSubscriber subscriber = new KafkaSubscriber(consumer, 10, true);
        Assert.assertNotNull(subscriber.receive());
        Assert.assertNull(subscriber.receive());
        verify(consumer, times(2)).commitAsync();
    }
}
