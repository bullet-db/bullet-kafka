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

@Slf4j
public class CertRefreshingSslEngineFactory implements SslEngineFactory {
    public static final String SSL_CERT_LOCATION_CONFIG = "ssl.cert.refreshing.cert.location";
    public static final String SSL_KEY_LOCATION_CONFIG = "ssl.cert.refreshing.key.location";
    public static final String SSL_KEY_REFRESH_INTERVAL_CONFIG = "ssl.cert.refreshing.refresh.interval.ms";

    // Package level for testing
    String athenzPublicCertLocation;
    String athenzPrivateKeyLocation;
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
        this.athenzPublicCertLocation = findFileOrThrow(configs, SSL_CERT_LOCATION_CONFIG);
        this.athenzPrivateKeyLocation = findFileOrThrow(configs, SSL_KEY_LOCATION_CONFIG);
        this.truststoreLocation = findFileOrThrow(configs, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        this.truststorePassword = (Password) configs.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        this.keyRefreshInterval = Integer.parseInt((String) configs.get(SSL_KEY_REFRESH_INTERVAL_CONFIG));

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
        throw new RuntimeException(this.getClass().getCanonicalName() + " only supports client-side SSLEngines, it " +
                                   "does not support createServerSslEngine().");
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
        // As of kafka-client 2.6 this method is only used on the server-side, so we don't need to return a real
        // keystore, but we will anyway just in case future kafka-client versions use it on the client side as well.
        try {
            return createKeyStore(athenzPublicCertLocation, athenzPrivateKeyLocation);
        } catch (Exception e) {
            log.error("Error creating keystore - this function should not be getting called on the client side.", e);
            return null;
        }
    }

    @Override
    public KeyStore truststore() {
        // As of kafka-client 2.6 this method is only used on the server-side, so we don't need to return a real
        // truststore, but we will anyway just in case future kafka-client versions use it on the client side as well.
        try {
            return getKeyStore(truststoreLocation, truststorePassword.value().toCharArray());
        } catch (Exception e) {
            log.error("Error creating truststore - this function should not be getting called on the client side.", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.sslContext = null;
    }

    private SSLContext createSSLContext() {
        try {
            KeyRefresher keyRefresher = generateKeyRefresher(truststoreLocation,
                                                             truststorePassword.value(),
                                                             athenzPublicCertLocation,
                                                             athenzPrivateKeyLocation);
            SSLContext sslContext = Utils.buildSSLContext(keyRefresher.getKeyManagerProxy(), keyRefresher.getTrustManagerProxy());
            keyRefresher.startup(keyRefreshInterval);
            log.info("Creating SSLContext with KeyManagerProxy and TrustManagerProxy that will be refreshed every {} ms.", keyRefreshInterval);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSLContext.", e);
        }
    }

    // Functions that proxy to other classes can be extended for testing

    protected KeyStore getKeyStore(String jksFilePath, char[] password) throws Exception {
        return Utils.getKeyStore(jksFilePath, password);
    }

    protected KeyStore createKeyStore(String athenzPublicCert, String athenzPrivateKey) throws Exception {
        return Utils.createKeyStore(athenzPublicCert, athenzPrivateKey);
    }

    protected KeyRefresher generateKeyRefresher(String trustStorePath, String trustStorePassword, String athenzPublicCert, String athenzPrivateKey) throws Exception {
        return Utils.generateKeyRefresher(trustStorePath, trustStorePassword, athenzPublicCert, athenzPrivateKey);
    }

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
