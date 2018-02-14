/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.yahoo.bullet.common.BulletConfig;
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
        Assert.assertNull(config.get("fake_setting"));
        Assert.assertEquals(config.get(KafkaConfig.BATCH_SIZE), "65536");
        Assert.assertEquals(config.get(KafkaConfig.REQUEST_TOPIC_NAME), "bullet.queries");
        Assert.assertEquals(config.get(KafkaConfig.REQUEST_TIMEOUT), "3000");
        Assert.assertEquals(config.get(KafkaConfig.CONSUMER_NAMESPACE + "request.timeout.ms"), "35000");
    }

    @Test
    public void testCopyPubSubConfig() {
        String randomString = TestUtils.getRandomString();
        BulletConfig config = new BulletConfig("src/test/resources/test_config.yaml");
        config.set(randomString, randomString);
        KafkaConfig kafkaConfig = new KafkaConfig(config);
        // Test copied property.
        Assert.assertEquals(config.get(randomString), randomString);
        Assert.assertEquals(kafkaConfig.get(randomString), randomString);
        // Test default properties.
        Assert.assertEquals(kafkaConfig.get(KafkaConfig.BATCH_SIZE), "65536");
        Assert.assertEquals(kafkaConfig.get(KafkaConfig.REQUEST_TOPIC_NAME), "bullet.queries");
    }

    @Test
    public void testMakeKafkaProperties() {
        KafkaConfig config = new KafkaConfig("");
        Set<String> keys = new HashSet<>(Collections.singleton(KafkaConfig.REQUEST_TOPIC_NAME));
        Map<String, Object> kafkaProperties = config.getAllWithPrefix(Optional.of(keys), KafkaConfig.KAFKA_NAMESPACE, true);

        String strippedName = KafkaConfig.REQUEST_TOPIC_NAME.substring(KafkaConfig.KAFKA_NAMESPACE.length());
        Assert.assertEquals(kafkaProperties.size(), 1);
        Assert.assertTrue(kafkaProperties.containsKey(strippedName));
        Assert.assertEquals(kafkaProperties.get(strippedName), "bullet.queries");
    }
}
