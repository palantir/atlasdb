/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.http;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.lock.RemoteLockService;

import feign.Client;
import feign.okhttp.OkHttpClient;
import okhttp3.CipherSuite;
import okhttp3.ConnectionPool;
import okhttp3.ConnectionSpec;
import okhttp3.Interceptor;
import okhttp3.Response;
import okhttp3.TlsVersion;

public final class FeignOkHttpClients {
    @VisibleForTesting
    static final String USER_AGENT_HEADER = "User-Agent";
    private static final int CONNECTION_POOL_SIZE = 100;
    private static final long KEEP_ALIVE_TIME_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    // See internal ticket PDS-50301, and/or #1680
    @VisibleForTesting
    static final Set<Class<?>> CLASSES_TO_NOT_RETRY = ImmutableSet.of(RemoteLockService.class);

    private static final ImmutableList<ConnectionSpec> CONNECTION_SPEC_WITH_CYPHER_SUITES = ImmutableList.of(
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
     * Returns a feign {@link Client} wrapping a {@link okhttp3.OkHttpClient} client with optionally
     * specified {@link SSLSocketFactory}.
     */
    public static <T> Client newOkHttpClient(
            Optional<SSLSocketFactory> sslSocketFactory,
            String userAgent,
            Class<T> clazz) {
        return newOkHttpClient(sslSocketFactory, userAgent, shouldAllowRetrying(clazz));
    }

    private static Client newOkHttpClient(
            Optional<SSLSocketFactory> sslSocketFactory,
            String userAgent,
            boolean retryOnConnectionFailure) {
        okhttp3.OkHttpClient.Builder builder = new okhttp3.OkHttpClient.Builder()
                .connectionSpecs(CONNECTION_SPEC_WITH_CYPHER_SUITES)
                .connectionPool(new ConnectionPool(CONNECTION_POOL_SIZE, KEEP_ALIVE_TIME_MILLIS, TimeUnit.MILLISECONDS))
                .hostnameVerifier((a, b) -> true)
                .retryOnConnectionFailure(retryOnConnectionFailure);
        if (sslSocketFactory.isPresent()) {
            builder.sslSocketFactory(sslSocketFactory.get());
        }
        builder.interceptors().add(new UserAgentAddingInterceptor(userAgent));
        return new OkHttpClient(builder.build());
    }

    @VisibleForTesting
    static <T> boolean shouldAllowRetrying(Class<T> clazz) {
        // Subclasses of this class should NOT be considered for retrying.
        return !CLASSES_TO_NOT_RETRY.contains(clazz);
    }

    private static final class UserAgentAddingInterceptor implements Interceptor {
        private final String userAgent;

        private UserAgentAddingInterceptor(String userAgent) {
            Preconditions.checkNotNull(userAgent, "User Agent should never be null.");
            this.userAgent = userAgent;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            okhttp3.Request requestWithUserAgent = chain.request()
                    .newBuilder()
                    .addHeader(USER_AGENT_HEADER, userAgent)
                    .build();
            return chain.proceed(requestWithUserAgent);
        }
    }
}
