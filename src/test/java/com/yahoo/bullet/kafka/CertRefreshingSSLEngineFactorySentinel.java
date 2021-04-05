/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.kafka;

import com.oath.auth.KeyRefresher;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CertRefreshingSSLEngineFactorySentinel extends CertRefreshingSSLEngineFactory {
    KeyRefresher keyRefresher;
    SSLEngine sslEngine;
    Map<String, List<Object>> utilFunctionsCalled;

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

    public void utilFunctionCalled(String function, List<Object> args) {
        if (utilFunctionsCalled == null) {
            utilFunctionsCalled = new HashMap<>();
        }
        utilFunctionsCalled.put(function, args);
    }
}
