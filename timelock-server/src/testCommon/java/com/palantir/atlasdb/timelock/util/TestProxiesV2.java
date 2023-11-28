/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.util;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.timelock.TestableTimelockServerV2;
import com.palantir.atlasdb.timelock.TimeLockServerHolderV2;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class TestProxiesV2 {

    private static final SslConfiguration SSL_CONFIGURATION =
            SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    public static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);

    private final String baseUri;
    private final List<TimeLockServerHolderV2> servers;
    private final ConcurrentMap<Object, Object> proxies = new ConcurrentHashMap<>();

    public TestProxiesV2(String baseUri, List<TestableTimelockServerV2> servers) {
        this.baseUri = baseUri;
        this.servers =
                servers.stream().map(TestableTimelockServerV2::serverHolder).collect(Collectors.toList());
    }

    public enum ProxyModeV2 {
        DIRECT {
            @Override
            int getPort(TimeLockServerHolderV2 serverHolder) {
                return serverHolder.getTimelockPort();
            }
        },
        WIREMOCK {
            @Override
            int getPort(TimeLockServerHolderV2 serverHolder) {
                return serverHolder.getTimelockWiremockPort();
            }
        };

        abstract int getPort(TimeLockServerHolderV2 serverHolder);
    }

    public void clearProxies() {
        proxies.clear();
    }

    public <T> T singleNode(TimeLockServerHolderV2 server, Class<T> serviceInterface, ProxyModeV2 proxyMode) {
        return singleNode(server, serviceInterface, true, proxyMode);
    }

    public <T> T singleNode(
            TimeLockServerHolderV2 server, Class<T> serviceInterface, boolean shouldRetry, ProxyModeV2 proxyMode) {
        String uri = getServerUri(server, proxyMode);
        List<Object> key = ImmutableList.of(serviceInterface, uri, "single", proxyMode);
        AuxiliaryRemotingParameters parameters = shouldRetry
                ? TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING
                : TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_NO_RETRYING;
        return (T) proxies.computeIfAbsent(
                key,
                ignored ->
                        AtlasDbHttpClients.createProxy(Optional.of(TRUST_CONTEXT), uri, serviceInterface, parameters));
    }

    public <T> T failover(Class<T> serviceInterface, ProxyModeV2 proxyMode) {
        List<String> uris =
                servers.stream().map(server -> getServerUri(server, proxyMode)).collect(Collectors.toList());

        List<Object> key = ImmutableList.of(serviceInterface, uris, "failover", proxyMode);
        return (T) proxies.computeIfAbsent(
                key,
                ignored -> AtlasDbHttpClients.createProxyWithFailover(
                        MetricsManagers.createForTests(),
                        ImmutableServerListConfig.builder()
                                .addAllServers(uris)
                                .sslConfiguration(SSL_CONFIGURATION)
                                .build(),
                        serviceInterface,
                        TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS_RETRYING));
    }

    private String getServerUri(TimeLockServerHolderV2 server, ProxyModeV2 proxyMode) {
        return baseUri + ":" + proxyMode.getPort(server);
    }
}
