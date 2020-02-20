/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.pubsub.Metadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;

public class KafkaMetadataTest {
    @Test
    public void testCreationWithoutMetadata() {
        TopicPartition topicPartition = new TopicPartition("foo", 4);
        KafkaMetadata metadata = new KafkaMetadata(topicPartition);
        Assert.assertNull(metadata.getSignal());
        Assert.assertNull(metadata.getContent());
        Assert.assertSame(metadata.getTopicPartition(), topicPartition);
        Assert.assertEquals(metadata.getTopicPartition(), new TopicPartition("foo", 4));
        metadata.setTopicPartition(null);
        Assert.assertNull(metadata.getTopicPartition());
    }

    @Test
    public void testCreationWithMetadata() {
        TopicPartition topicPartition = new TopicPartition("foo", 4);
        Metadata custom = new Metadata(Metadata.Signal.CUSTOM, new HashMap<>());
        KafkaMetadata metadata = new KafkaMetadata(custom, topicPartition);
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.CUSTOM);
        Assert.assertEquals(metadata.getContent(), Collections.emptyMap());
        Assert.assertSame(metadata.getTopicPartition(), topicPartition);
        Assert.assertEquals(metadata.getTopicPartition(), new TopicPartition("foo", 4));
    }
}
