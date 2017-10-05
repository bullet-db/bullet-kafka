/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KafkaConfigTest {
    @Test
    public void testDefaultFileKafkaSettings() {
        KafkaConfig config = new KafkaConfig("");
        Assert.assertTrue(config.get("fake_setting") == null);
        Assert.assertTrue(config.get(KafkaConfig.BATCH_SIZE).equals("65536"));
        Assert.assertTrue(config.get(KafkaConfig.REQUEST_TOPIC_NAME).equals("bullet.queries"));
    }

    @Test
    public void testCopyPubSubConfig() {
        String randomString = TestUtils.getRandomString();
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(randomString, randomString);
        KafkaConfig kafkaConfig = new KafkaConfig(config);
        // Test copied property.
        Assert.assertTrue(config.get(randomString).equals(randomString));
        Assert.assertTrue(kafkaConfig.get(randomString).equals(randomString));
        // Test default properties.
        Assert.assertTrue(kafkaConfig.get(KafkaConfig.BATCH_SIZE).equals("65536"));
        Assert.assertEquals(kafkaConfig.get(KafkaConfig.REQUEST_TOPIC_NAME), "bullet.queries");
    }

    @Test
    public void testMakeKafkaProperties() {
        KafkaConfig config = new KafkaConfig("");
        Set<String> keys = new HashSet<>(Collections.singleton(KafkaConfig.REQUEST_TOPIC_NAME));
        Map<String, Object> kafkaProperties = config.getAllWithPrefix(Optional.of(keys), KafkaPubSub.SETTING_PREFIX, true);

        String strippedName = KafkaConfig.REQUEST_TOPIC_NAME.substring(KafkaPubSub.SETTING_PREFIX.length());
        Assert.assertEquals(kafkaProperties.size(), 1);
        Assert.assertTrue(kafkaProperties.containsKey(strippedName));
        Assert.assertEquals(kafkaProperties.get(strippedName), "bullet.queries");
    }
}
