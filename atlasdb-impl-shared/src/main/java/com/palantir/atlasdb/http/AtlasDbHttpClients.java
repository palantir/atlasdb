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

import javax.net.ssl.SSLSocketFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import feign.Client;
import feign.Contract;
import feign.Feign;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import feign.okhttp.OkHttpClient;

public class AtlasDbHttpClients {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Contract contract = new JAXRSContract();
    private static final Encoder encoder = new JacksonEncoder(mapper);
    private static final Decoder decoder = new TextDelegateDecoder(new JacksonDecoder(mapper));

    /**
     * Constructs a dynamic proxy for the specified type, using the supplied SSL factory if is present, and feign {@link
     * feign.Client.Default} HTTP client.
     */
    public static <T> T createRemoteProxy(Optional<SSLSocketFactory> sslSocketFactory, String uri, Class<T> type) {
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .client(newOkHttpClient(sslSocketFactory))
                .target(type, uri);
    }

    /**
     * Constructs a list, corresponding to the iteration order of the supplied endpoints, of dynamic proxies for the
     * specified type, using the supplied SSL factory if it is present.
     */
    public static <T> List<T> createRemoteProxies(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        List<T> ret = Lists.newArrayListWithCapacity(endpointUris.size());
        for (String uri : endpointUris) {
            ret.add(createRemoteProxy(sslSocketFactory, uri, type));
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
    public static <T> T createRemoteProxyWithFailover(
            Optional<SSLSocketFactory> sslSocketFactory, Collection<String> endpointUris, Class<T> type) {
        FailoverFeignTarget<T> failoverFeignTarget = new FailoverFeignTarget<>(endpointUris, type);
        Client client = failoverFeignTarget.wrapClient(newOkHttpClient(sslSocketFactory));
        return Feign.builder()
                .contract(contract)
                .encoder(encoder)
                .decoder(decoder)
                .client(client)
                .retryer(failoverFeignTarget)
                .target(failoverFeignTarget);
    }

    /**
     * Returns a feign {@link Client} wrapping a {@link com.squareup.okhttp.OkHttpClient} client with optionally
     * specified {@link SSLSocketFactory}.
     */
    private static Client newOkHttpClient(Optional<SSLSocketFactory> sslSocketFactory) {
        com.squareup.okhttp.OkHttpClient client = new com.squareup.okhttp.OkHttpClient();
        client.setSslSocketFactory(sslSocketFactory.orNull());
        return new OkHttpClient(client);
    }

}
