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

import java.net.ProxySelector;
import java.util.Collection;
import java.util.Optional;

import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

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

public final class AtlasDbFeignTargetFactory {
    private static final Request.Options DEFAULT_FEIGN_OPTIONS = new Request.Options();

    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new Jdk8Module());
    private static final Contract contract = new JAXRSContract();
    private static final Encoder encoder = new JacksonEncoder(mapper);
    private static final Decoder decoder = new TextDelegateDecoder(
            new OptionalAwareDecoder(new JacksonDecoder(mapper)));
    private static final ErrorDecoder errorDecoder = new AtlasDbErrorDecoder();

    private AtlasDbFeignTargetFactory() {
        // factory
    }

    public static <T> T createProxy(
            Optional<SSLSocketFactory> sslSocketFactory,
            String uri,
            Class<T> type,
            String userAgent) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(errorDecoder)
                .retryer(new InterruptHonoringRetryer())
                .client(FeignOkHttpClients.newOkHttpClient(sslSocketFactory, Optional.empty(), userAgent))
                .target(type, uri);
    }

    public static <T> T createRsProxy(
            Optional<SSLSocketFactory> sslSocketFactory,
            String uri,
            Class<T> type,
            String userAgent) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .errorDecoder(new RsErrorDecoder())
                .client(FeignOkHttpClients.newOkHttpClient(sslSocketFactory, Optional.empty(),  userAgent))
                .target(type, uri);
    }

    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Class<T> type,
            String userAgent) {
        return createProxyWithFailover(
                sslSocketFactory,
                proxySelector,
                endpointUris,
                DEFAULT_FEIGN_OPTIONS,
                FailoverFeignTarget.DEFAULT_MAX_BACKOFF_MILLIS,
                type,
                userAgent);
    }

    public static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            int feignConnectTimeout,
            int feignReadTimeout,
            int maxBackoffMillis,
            Class<T> type,
            String userAgent) {
        return createProxyWithFailover(
                sslSocketFactory,
                proxySelector,
                endpointUris,
                new Request.Options(feignConnectTimeout, feignReadTimeout),
                maxBackoffMillis,
                type,
                userAgent);
    }

    private static <T> T createProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory,
            Optional<ProxySelector> proxySelector,
            Collection<String> endpointUris,
            Request.Options feignOptions,
            int maxBackoffMillis,
            Class<T> type,
            String userAgent) {
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(endpointUris, maxBackoffMillis, type);
        Client client = failoverFeignTarget.wrapClient(
                FeignOkHttpClients.newOkHttpClient(sslSocketFactory, proxySelector, userAgent));
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

}
