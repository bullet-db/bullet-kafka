/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.oath.auth.KeyRefresher;
import org.apache.kafka.common.config.SslConfigs;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.kafka.common.config.types.Password;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.kafka.KafkaConfig.SSL_CERT_LOCATION;
import static com.yahoo.bullet.kafka.KafkaConfig.SSL_KEY_LOCATION;
import static com.yahoo.bullet.kafka.KafkaConfig.SSL_KEY_REFRESH_INTERVAL;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;

public class CertRefreshingSSLEngineFactoryTest {
    private static final String FAKE_CERT = System.getProperty("user.dir") + "/src/test/resources/fake_cert.txt";

    private CertRefreshingSSLEngineFactorySentinel factorySentinel;
    private InstantiableCertRefreshingSSLEngineFactory instantiatedFactory;
    private Map<String, Object> conf;

    @BeforeMethod
    public void setup() {
        this.factorySentinel = new CertRefreshingSSLEngineFactorySentinel();
        this.instantiatedFactory = new InstantiableCertRefreshingSSLEngineFactory();
        this.conf = getBasicConf();
    }

    @Test
    public void testConfigureSuccess() {
        factorySentinel.configure(conf);

        Assert.assertEquals(factorySentinel.publicCertLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.privateKeyLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.truststoreLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.truststorePassword.value(), "password");
        Assert.assertEquals(factorySentinel.keyRefreshInterval, 1000);
        Assert.assertNull(factorySentinel.cipherSuites);
        Assert.assertNull(factorySentinel.enabledProtocols);
        Assert.assertEquals(factorySentinel.keyRefreshInterval, 1000);

        KeyRefresher mock = factorySentinel.getKeyRefresher();
        verify(mock).getKeyManagerProxy();
        verify(mock).getTrustManagerProxy();
        verify(mock).startup(1000);
    }

    @Test
    public void testConfigureOptionalValues() throws Exception {
        List<String> cipherSuites = Arrays.asList("some", "cipher", "suites");
        List<String> enabledProtocols = Arrays.asList("some", "enabled", "protocols");
        conf.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuites);
        conf.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);
        factorySentinel.configure(conf);

        Assert.assertEquals(factorySentinel.publicCertLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.privateKeyLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.truststoreLocation, FAKE_CERT);
        Assert.assertEquals(factorySentinel.truststorePassword.value(), "password");
        Assert.assertEquals(factorySentinel.keyRefreshInterval, 1000);
        Assert.assertEquals(factorySentinel.cipherSuites, cipherSuites.toArray());
        Assert.assertEquals(factorySentinel.enabledProtocols, enabledProtocols.toArray());
        Assert.assertEquals(factorySentinel.keyRefreshInterval, 1000);

        KeyRefresher mock = factorySentinel.getKeyRefresher();
        verify(mock).getKeyManagerProxy();
        verify(mock).getTrustManagerProxy();
        verify(mock).startup(1000);
    }

    @Test
    public void testConfigureFileNotFound() {
        try {
            conf.put(SSL_TRUSTSTORE_LOCATION_CONFIG, "garbage");
            factorySentinel.configure(conf);
            Assert.fail("configure() should throw with a garbage setting for truststore.");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("garbage does not exist. Check the setting"));
        }
    }

    @Test
    public void testCreateClientSslEngine() {
        factorySentinel.configure(conf);

        SSLEngine mock = factorySentinel.createClientSslEngine("peerHost", 88, "someEndpointIdentification");

        verify(mock, times(0)).setEnabledCipherSuites(any());
        verify(mock, times(0)).setEnabledProtocols(any());
        verify(mock).setUseClientMode(true);
        Assert.assertEquals(mock.getSSLParameters().getEndpointIdentificationAlgorithm(), "someEndpointIdentification");
    }

    @Test
    public void testCreateClientSslEngineWithParams() {
        List<String> cipherSuites = Arrays.asList("some", "cipher", "suites");
        List<String> enabledProtocols = Arrays.asList("some", "enabled", "protocols");
        conf.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuites);
        conf.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, enabledProtocols);
        factorySentinel.configure(conf);

        SSLEngine mock = factorySentinel.createClientSslEngine("peerHost", 88, "someEndpointIdentification");

        verify(mock).setEnabledCipherSuites((String[]) cipherSuites.toArray());
        verify(mock).setEnabledProtocols((String[]) enabledProtocols.toArray());
        verify(mock).setUseClientMode(true);
        Assert.assertEquals(mock.getSSLParameters().getEndpointIdentificationAlgorithm(), "someEndpointIdentification");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCreateServerSslEngine() {
        factorySentinel.configure(conf);
        factorySentinel.createServerSslEngine("peerHost", 88);
    }

    @Test
    public void testShouldBeRebuilt() {
        factorySentinel.configure(conf);
        Assert.assertFalse(factorySentinel.shouldBeRebuilt(null));
        Assert.assertFalse(factorySentinel.shouldBeRebuilt(conf));
    }

    @Test
    public void testReconfigurableConfigs() {
        factorySentinel.configure(conf);
        Assert.assertTrue(factorySentinel.reconfigurableConfigs().isEmpty());
    }

    @Test
    public void testKeystore() {
        factorySentinel.configure(conf);
        factorySentinel.keystore();

        List<Object> expectedParamsPassedToUtilsClass = Arrays.asList(FAKE_CERT, FAKE_CERT);
        Assert.assertEquals(factorySentinel.utilFunctionsCalled.size(), 2);
        Assert.assertNotNull(factorySentinel.utilFunctionsCalled.get("generateKeyRefresher"));
        Assert.assertEquals(factorySentinel.utilFunctionsCalled.get("createKeyStore"), expectedParamsPassedToUtilsClass);
    }

    @Test
    public void testTruststore() {
        factorySentinel.configure(conf);
        factorySentinel.truststore();

        Assert.assertEquals(factorySentinel.utilFunctionsCalled.size(), 2);
        Assert.assertNotNull(factorySentinel.utilFunctionsCalled.get("generateKeyRefresher"));

        List<Object> expectedParamsPassedToUtilsClass = Arrays.asList(FAKE_CERT, "password".toCharArray());
        List<Object> actualParamsPassedToUtilsClass = factorySentinel.utilFunctionsCalled.get("getKeyStore");
        Assert.assertEquals(actualParamsPassedToUtilsClass.get(0), expectedParamsPassedToUtilsClass.get(0));
        Assert.assertEquals(new String((char[]) actualParamsPassedToUtilsClass.get(1)), new String((char[]) expectedParamsPassedToUtilsClass.get(1)));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testConfigThrows() throws Exception {
        new CertRefreshingSSLEngineFactory().configure(conf);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testKeystoreThrows() throws Exception {
        new CertRefreshingSSLEngineFactory().keystore();
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testTruststoreThrows() throws Exception {
        new CertRefreshingSSLEngineFactory().truststore();
    }

    @Test
    public void testConfigAllFilesFound() throws Exception {
        instantiatedFactory.configure(conf);
        Assert.assertNotNull(instantiatedFactory.publicCertLocation);
        Assert.assertNotNull(instantiatedFactory.privateKeyLocation);
        Assert.assertNotNull(instantiatedFactory.truststoreLocation);
        Assert.assertNotNull(instantiatedFactory.truststorePassword);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testConfigFileNotFound() throws Exception {
        conf.put(SSL_CERT_LOCATION, "does-not-exist");
        instantiatedFactory.configure(conf);
    }

    @Test
    public void testGetKeystore() throws Exception {
        KeyStore keyStore = instantiatedFactory.getKeyStore("somePath", "somePassword".toCharArray());
        Assert.assertNotNull(keyStore);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCreateSSLEngineThrowsWithNoContext() throws Exception {
        instantiatedFactory.createSSLEngine("somePeerHost", 88);
    }

    @Test
    public void testClose() throws Exception {
        factorySentinel.configure(conf);
        Assert.assertNotNull(factorySentinel.sslContext);
        factorySentinel.close();
        Assert.assertNull(factorySentinel.sslContext);
    }

    private static Map<String, Object> getBasicConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(SSL_CERT_LOCATION, FAKE_CERT);
        conf.put(SSL_KEY_LOCATION, FAKE_CERT);
        conf.put(SSL_TRUSTSTORE_LOCATION_CONFIG, FAKE_CERT);
        conf.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, new Password("password"));
        conf.put(SSL_KEY_REFRESH_INTERVAL, "1000");

        return conf;
    }

    private static class InstantiableCertRefreshingSSLEngineFactory extends CertRefreshingSSLEngineFactory {
        @Override
        protected SSLContext createSSLContext() {
            return null;
        }
    }

    private static class CertRefreshingSSLEngineFactorySentinel extends CertRefreshingSSLEngineFactory {
        private KeyRefresher keyRefresher;
        private SSLEngine sslEngine;
        private Map<String, List<Object>> utilFunctionsCalled;

        public KeyRefresher getKeyRefresher() {
            return keyRefresher;
        }

        @Override
        protected KeyStore getKeyStore(String jksFilePath, char[] password) throws Exception {
            utilFunctionCalled("getKeyStore", Arrays.asList(jksFilePath, password));
            return null;
        }

        @Override
        protected KeyStore createKeyStore(String publicCertLocation, String privateKeyLocation) throws Exception {
            utilFunctionCalled("createKeyStore", Arrays.asList(publicCertLocation, privateKeyLocation));
            return null;
        }

        @Override
        protected KeyRefresher generateKeyRefresher(String trustStorePath, String trustStorePassword, String publicCertLocation, String privateKeyLocation) throws Exception {
            utilFunctionCalled("generateKeyRefresher", Arrays.asList(trustStorePath, trustStorePassword, publicCertLocation, privateKeyLocation));
            if (keyRefresher == null) {
                this.keyRefresher = mock(KeyRefresher.class);
            }
            return keyRefresher;
        }

        @Override
        protected SSLEngine createSSLEngine(String peerHost, int peerPort) {
            if (sslEngine == null) {
                SSLParameters sslParams = new SSLParameters();
                sslEngine = mock(SSLEngine.class);
                doReturn(sslParams).when(sslEngine).getSSLParameters();
            }
            return sslEngine;
        }

        private void utilFunctionCalled(String function, List<Object> args) {
            if (utilFunctionsCalled == null) {
                utilFunctionsCalled = new HashMap<>();
            }
            utilFunctionsCalled.put(function, args);
        }
    }
}
