/**
 * Copyright 2015 Palantir Technologies
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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.squareup.okhttp.ConnectionPool;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.Request;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.codec.ErrorDecoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import feign.okhttp.OkHttpClient;

public final class AtlasDbHttpClients {

    private static final int CONNECTION_POOL_SIZE = 100;
    private static final long KEEP_ALIVE_TIME_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
    private static final int ONE_MILLISECOND = 1;
    private static final Request.Options DEFAULT_FEIGN_OPTIONS = new Request.Options();

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Contract contract = new JAXRSContract();
    private static final Encoder encoder = new JacksonEncoder(mapper);
    private static final Decoder decoder = new TextDelegateDecoder(new JacksonDecoder(mapper));
    private static final ErrorDecoder errorDecoder = new AtlasDbErrorDecoder();

    private AtlasDbHttpClients() {
        // Utility class
    }

    /**
     * Constructs a dynamic proxy for the specified type, using the supplied SSL factory if is present, and feign {@link
     * feign.Client.Default} HTTP client.
     */
    public static <T> T createProxy(Optional<SSLSocketFactory> sslSocketFactory, String uri, Class<T> type) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .client(newOkHttpClient(sslSocketFactory))
                .target(type, uri);
    }

    /**
     * Constructs a list, corresponding to the iteration order of the supplied endpoints, of dynamic proxies for the
     * specified type, using the supplied SSL factory if it is present.
     */
    public static <T> List<T> createProxies(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        List<T> ret = Lists.newArrayListWithCapacity(endpointUris.size());
        for (String uri : endpointUris) {
            ret.add(createProxy(sslSocketFactory, uri, type));
        }
        return ret;
    }

    /**
     * Constructs an HTTP-invoking dynamic proxy for the specified type that will cycle through the list of supplied
     * endpoints after encountering an exception or connection failure, using the supplied SSL factory if it is
     * present.
     * <p>
     * Failover will continue to cycle through the supplied endpoint list indefinitely.
     */
    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        return createProxyWithFailover(
                sslSocketFactory,
                endpointUris,
                DEFAULT_FEIGN_OPTIONS,
                FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                type);
    }

    /**
     * @param feignOptions      Options to configure Feign timeouts.
     * @param maxBackoffMillis  Passed through to the FailoverFeignTarget, this configures the maximum time that a
     *                          backoff will be for.
     */
    private static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris,
            Request.Options feignOptions, int maxBackoffMillis, Class<T> type) {
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(endpointUris, maxBackoffMillis, type);
        Client client = failoverFeignTarget.wrapClient(newOkHttpClient(sslSocketFactory));
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .client(client)
                .retryer(failoverFeignTarget)
                .options(feignOptions)
                .target(failoverFeignTarget);
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        Request.Options options = new Request.Options(10000, 10000);
        return createProxyWithFailover(
                sslSocketFactory,
                endpointUris,
                options,
                ONE_MILLISECOND,
                type);
    }

    /**
     * Returns a feign {@link Client} wrapping a {@link com.squareup.okhttp.OkHttpClient} client with optionally
     * specified {@link SSLSocketFactory}.
     */
    private static Client newOkHttpClient(Optional<SSLSocketFactory> sslSocketFactory) {
        com.squareup.okhttp.OkHttpClient client = new com.squareup.okhttp.OkHttpClient();
        client.setConnectionPool(new ConnectionPool(CONNECTION_POOL_SIZE, KEEP_ALIVE_TIME_MILLIS));
        client.setSslSocketFactory(sslSocketFactory.orNull());
        return new OkHttpClient(client);
    }
}
