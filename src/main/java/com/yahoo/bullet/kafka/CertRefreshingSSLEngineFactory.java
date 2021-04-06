/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.oath.auth.KeyRefresher;
import com.oath.auth.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.utils.SecurityUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This implementation of Kafka's {@link SslEngineFactory} will periodically check the certs used for SSL
 * authentication and rebuild the keystore, and/or reload the truststore if changes have been made to the
 * files on disk. This functionality is only supported in Kafka 2.6 or later. This class is a client-side
 * implementation only - server-side functions like `createServerSslEngine()` are not supported. See
 * `src/main/resources/bullet_kafka_defaults.yaml` for a list of required settings.
 */
@Slf4j
public class CertRefreshingSSLEngineFactory implements SslEngineFactory {
    // Package level for testing
    String publicCertLocation;
    String privateKeyLocation;
    String truststoreLocation;
    Password truststorePassword;
    int keyRefreshInterval;
    String[] cipherSuites = null;
    String[] enabledProtocols = null;
    SSLContext sslContext;

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Configuring {}...", this.getClass().getCanonicalName());
        this.publicCertLocation = findFileOrThrow(configs, KafkaConfig.SSL_CERT_LOCATION);
        this.privateKeyLocation = findFileOrThrow(configs, KafkaConfig.SSL_KEY_LOCATION);
        this.truststoreLocation = findFileOrThrow(configs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        this.truststorePassword = (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        this.keyRefreshInterval = (Integer) configs.get(KafkaConfig.SSL_KEY_REFRESH_INTERVAL);

        SecurityUtils.addConfiguredSecurityProviders(configs);

        List<String> cipherSuitesList = (List<String>) configs.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        if (cipherSuitesList != null && !cipherSuitesList.isEmpty()) {
            this.cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
        }

        List<String> enabledProtocolsList = (List<String>) configs.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        if (enabledProtocolsList != null && !enabledProtocolsList.isEmpty()) {
            this.enabledProtocols = enabledProtocolsList.toArray(new String[enabledProtocolsList.size()]);
        }

        this.sslContext = createSSLContext();
    }

    @Override
    public SSLEngine createClientSslEngine(String peerHost, int peerPort, String endpointIdentification) {
        SSLEngine sslEngine = createSSLEngine(peerHost, peerPort);

        if (cipherSuites != null) {
            sslEngine.setEnabledCipherSuites(cipherSuites);
        }
        if (enabledProtocols != null) {
            sslEngine.setEnabledProtocols(enabledProtocols);
        }

        sslEngine.setUseClientMode(true);

        SSLParameters sslParams = sslEngine.getSSLParameters();
        sslParams.setEndpointIdentificationAlgorithm(endpointIdentification);
        sslEngine.setSSLParameters(sslParams);

        return sslEngine;
    }

    @Override
    public SSLEngine createServerSslEngine(String peerHost, int peerPort) {
        throw new UnsupportedOperationException("This only supports client-side SSLEngines, it does not support createServerSslEngine().");
    }

    @Override
    public boolean shouldBeRebuilt(Map<String, Object> nextConfigs) {
        // This is only used for server-side SslEngineFactories, so we can just return false here on the client-side.
        // Also, even if it's called on the client-side in future kafka-client versions, we cannot allow this
        // class to be rebuilt at the SslChannelBuilder / SslFactory level because then the settings
        // (e.g. keystore/truststore paths) in the KeyManager and TrustManager (which live in the SSLEngine) could get
        // out-of-sync with the settings in the re-built instance of this class that is being used in the
        // SslChannelBuilder / SslFactory
        return false;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    @Override
    public KeyStore keystore() {
        try {
            return createKeyStore(publicCertLocation, privateKeyLocation);
        } catch (Exception e) {
            throw new RuntimeException("Error creating keystore.", e);
        }
    }

    @Override
    public KeyStore truststore() {
        try {
            return getKeyStore(truststoreLocation, truststorePassword.value().toCharArray());
        } catch (Exception e) {
            throw new RuntimeException("Error creating truststore.", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.sslContext = null;
    }

    // Functions that proxy to other classes or require real certs can be extended for testing

    /**
     * Create an SSLContext.
     *
     * @return The {@link SSLContext}.
     */
    protected SSLContext createSSLContext() {
        try {
            KeyRefresher keyRefresher = generateKeyRefresher(truststoreLocation,
                                                             truststorePassword.value(),
                                                             publicCertLocation,
                                                             privateKeyLocation);
            SSLContext sslContext = Utils.buildSSLContext(keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
            keyRefresher.startup(keyRefreshInterval);
            log.info("Creating SSLContext that will refresh keys every {} ms.", keyRefreshInterval);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSLContext.", e);
        }
    }

    /**
     * Generate a keystore from the given jks file and password.
     *
     * @param jksFilePath The path to the jks file.
     * @param password The password for the jks file.
     * @return The keystore.
     * @throws Exception if the keystore cannot be generated with the given parameters.
     */
    protected KeyStore getKeyStore(String jksFilePath, char[] password) throws Exception {
        return Utils.getKeyStore(jksFilePath, password);
    }

    /**
     * Generate a keystore from the given public cert and private key.
     *
     * @param publicCertLocation The location of the public cert.
     * @param privateKeyLocation The location of the private key.
     * @return The keystore.
     * @throws Exception if the keystore cannot be generated with the given parameters.
     */
    protected KeyStore createKeyStore(String publicCertLocation, String privateKeyLocation) throws Exception {
        return Utils.createKeyStore(publicCertLocation, privateKeyLocation);
    }

    /**
     * Generate a {@link KeyRefresher}.
     *
     * @param trustStorePath The path of the truststore file.
     * @param trustStorePassword The password for the truststore.
     * @param publicCertLocation The path of the public cert.
     * @param privateKeyLocation The path of the private key.
     * @return The KeyRefresher.
     * @throws Exception if the KeyRefresher cannot be generated with the provided parameters.
     */
    protected KeyRefresher generateKeyRefresher(String trustStorePath, String trustStorePassword, String publicCertLocation, String privateKeyLocation) throws Exception {
        return Utils.generateKeyRefresher(trustStorePath, trustStorePassword, publicCertLocation, privateKeyLocation);
    }

    /**
     * Create an {@link SSLEngine}.
     *
     * @param peerHost The peer host.
     * @param peerPort The peer port.
     * @return The SSLEngine.
     */
    protected SSLEngine createSSLEngine(String peerHost, int peerPort) {
        return sslContext.createSSLEngine(peerHost, peerPort);
    }

    private String findFileOrThrow(Map<String, ?> configs, String key) {
        String path = (String) configs.get(key);
        if (isAbsolutePathThatExists(path)) {
            log.debug("Verified file exists: {}", path);
            return path;
        }
        // If path is local, or an absolute path that does not exist, try the filename in the current working directory
        String pathInCurrentDirectory = pathInCurrentDirectory(path);
        if (fileExists(pathInCurrentDirectory)) {
            log.info("Could not find file with path '{}', using file in current directory instead: '{}'.", path, pathInCurrentDirectory);
            return pathInCurrentDirectory;
        }
        log.error("The provided path '{}' does not exist. The file in the current working directory '{}' also does " +
                  "not exist. Check the provided setting for '{}'.", path, pathInCurrentDirectory, key);
        throw new RuntimeException(new FileNotFoundException(path + " does not exist. Check the setting: " + key));
    }

    private boolean isAbsolutePathThatExists(String path) {
        File file = new File(path);
        return file.isAbsolute() && file.exists() && !file.isDirectory();
    }

    private boolean fileExists(String path) {
        File file = new File(path);
        return file.exists() && !file.isDirectory();
    }

    private String pathInCurrentDirectory(String path) {
        String fileName = Paths.get(path).getFileName().toString();
        return Paths.get(fileName).toAbsolutePath().toString();
    }
}
