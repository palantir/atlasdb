/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.net.ProxySelector;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.config.ssl.TrustContext;

import feign.Client;
import feign.okhttp.OkHttpClient;
import okhttp3.CipherSuite;
import okhttp3.ConnectionPool;
import okhttp3.ConnectionSpec;
import okhttp3.TlsVersion;

@SuppressWarnings("OptionalOrElseMethodInvocation")
public final class FeignOkHttpClients {
    private static final int CONNECTION_POOL_SIZE = 100;
    private static final long KEEP_ALIVE_TIME_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    /**
     * @deprecated Do not use; this method may be removed at any time. It is purely for internal benchmarking, which
     * adds additional settings to the http clients.
     */
    @Deprecated
    public static volatile Consumer<okhttp3.OkHttpClient.Builder> globalClientSettings = (client) -> { };

    public static final ImmutableList<ConnectionSpec> CONNECTION_SPEC_WITH_CYPHER_SUITES = ImmutableList.of(
            new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
                    .tlsVersions(TlsVersion.TLS_1_2)
                    .cipherSuites(
                            // This GCM cipher suite is for HTTP/2 over TLS1.2 as clients have to
                            // enable at least one cipher suite not in the blacklist.
                            // (https://http2.github.io/http2-spec/index.html#BadCipherSuites)
                            // Timelock server will support HTTP/2 connections, and this will ensure
                            // all AtlasDB clients have one supported cipher suite.
                            // See also:
                            //    - https://http2.github.io/http2-spec/index.html#rfc.section.9.2.2
                            CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
                            // In an ideal world, we'd use GCM suites, but they're an order of
                            // magnitude slower than the CBC suites, which have JVM optimizations
                            // already. We should revisit with JDK9.
                            // See also:
                            //  - http://openjdk.java.net/jeps/246
                            //  - https://bugs.openjdk.java.net/secure/attachment/25422/GCM%20Analysis.pdf
                            // CipherSuite.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
                            // CipherSuite.TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384,
                            // CipherSuite.TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256,
                            // CipherSuite.TLS_RSA_WITH_AES_256_GCM_SHA384,
                            // CipherSuite.TLS_RSA_WITH_AES_128_GCM_SHA256,
                            CipherSuite.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,
                            CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
                            CipherSuite.TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384,
                            CipherSuite.TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256,
                            CipherSuite.TLS_RSA_WITH_AES_128_CBC_SHA256,
                            CipherSuite.TLS_RSA_WITH_AES_256_CBC_SHA256,
                            CipherSuite.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
                            CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
                            CipherSuite.TLS_ECDH_RSA_WITH_AES_256_CBC_SHA,
                            CipherSuite.TLS_ECDH_RSA_WITH_AES_128_CBC_SHA,
                            CipherSuite.TLS_RSA_WITH_AES_256_CBC_SHA,
                            CipherSuite.TLS_RSA_WITH_AES_128_CBC_SHA,
                            CipherSuite.TLS_EMPTY_RENEGOTIATION_INFO_SCSV)
                    .build(),
            ConnectionSpec.CLEARTEXT);

    private FeignOkHttpClients() {
        // factory
    }

    /**
     * Returns a Feign {@link Client} wrapping an {@link okhttp3.OkHttpClient}. This {@link Client} recreates
     * itself in the event that either {@link CounterBackedRefreshingClient#DEFAULT_REQUEST_COUNT_BEFORE_REFRESH}
     * requests have been made, or if {@link ExceptionCountingRefreshingClient#DEFAULT_EXCEPTION_COUNT_BEFORE_REFRESH}
     * consecutive exceptions have been thrown by the underlying client.
     */
    public static Client newRefreshingOkHttpClient(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            String userAgent,
            boolean limitPayloadSize) {
        Supplier<Client> clientSupplier = () -> CounterBackedRefreshingClient.createRefreshingClient(
                () -> newOkHttpClient(trustContext, proxySelector, userAgent, limitPayloadSize));

        return ExceptionCountingRefreshingClient.createRefreshingClient(clientSupplier);
    }

    /**
     * Returns a feign {@link Client} wrapping a {@link okhttp3.OkHttpClient} client with optionally
     * specified {@link SSLSocketFactory}.
     */
    private static Client newOkHttpClient(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            String userAgent,
            boolean limitPayloadSize) {
        return new OkHttpClient(newRawOkHttpClient(trustContext, proxySelector, userAgent, limitPayloadSize));
    }

    @VisibleForTesting
    static okhttp3.OkHttpClient newRawOkHttpClient(
            Optional<TrustContext> trustContext,
            Optional<ProxySelector> proxySelector,
            String userAgent,
            boolean limitPayloadSize) {
        // Don't allow retrying on connection failures - see ticket #2194
        okhttp3.OkHttpClient.Builder builder = new okhttp3.OkHttpClient.Builder()
                .connectionSpecs(CONNECTION_SPEC_WITH_CYPHER_SUITES)
                .connectionPool(new ConnectionPool(CONNECTION_POOL_SIZE, KEEP_ALIVE_TIME_MILLIS, TimeUnit.MILLISECONDS))
                .proxySelector(proxySelector.orElse(ProxySelector.getDefault()))
                .retryOnConnectionFailure(false);
        if (trustContext.isPresent()) {
            builder.sslSocketFactory(trustContext.get().sslSocketFactory(), trustContext.get().x509TrustManager());
        }
        if (limitPayloadSize) {
            builder.interceptors().add(AtlasDbInterceptors.REQUEST_PAYLOAD_LIMITER);
        }
        builder.interceptors().add(new AtlasDbInterceptors.UserAgentAddingInterceptor(userAgent));

        globalClientSettings.accept(builder);
        return builder.build();
    }


}
