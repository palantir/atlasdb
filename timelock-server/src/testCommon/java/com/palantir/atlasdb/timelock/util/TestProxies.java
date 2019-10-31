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

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.http.AtlasDbHttpClients;
import com.palantir.atlasdb.http.TestProxyUtils;
import com.palantir.atlasdb.timelock.TestableTimelockServer;
import com.palantir.atlasdb.timelock.TimeLockServerHolder;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.conjure.java.config.ssl.SslSocketFactories;
import com.palantir.conjure.java.config.ssl.TrustContext;

public class TestProxies {

    public static final SslConfiguration SSL_CONFIGURATION
            = SslConfiguration.of(Paths.get("var/security/trustStore.jks"));
    public static final TrustContext TRUST_CONTEXT = SslSocketFactories.createTrustContext(SSL_CONFIGURATION);

    private final String baseUri;
    private final List<TimeLockServerHolder> servers;
    private final ConcurrentMap<Object, Object> proxies = Maps.newConcurrentMap();

    public TestProxies(String baseUri, List<TestableTimelockServer> servers) {
        this.baseUri = baseUri;
        this.servers = servers.stream()
                .map(TestableTimelockServer::serverHolder)
                .collect(Collectors.toList());
    }

    public <T> T singleNodeForClient(String client, TimeLockServerHolder server, Class<T> serviceInterface) {
        return singleNode(serviceInterface, getServerUriForClient(client, server));
    }

    public <T> T singleNode(TimeLockServerHolder server, Class<T> serviceInterface) {
        return singleNode(serviceInterface, getServerUri(server));
    }

    public <T> T singleNode(Class<T> serviceInterface, String uri) {
        List<Object> key = ImmutableList.of(serviceInterface, uri, "single");
        return (T) proxies.computeIfAbsent(key, ignored -> AtlasDbHttpClients.createProxy(
                MetricsManagers.createForTests(),
                Optional.of(TRUST_CONTEXT),
                uri,
                serviceInterface,
                TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS));
    }

    public <T> T failoverForClient(String client, Class<T> serviceInterface) {
        return failover(serviceInterface, getServerUrisForClient(client));
    }

    public <T> T failover(Class<T> serviceInterface, List<String> uris) {
        List<Object> key = ImmutableList.of(serviceInterface, uris, "failover");
        return (T) proxies.computeIfAbsent(key, ignored -> AtlasDbHttpClients.createProxyWithFailover(
                new MetricRegistry(),
                ImmutableServerListConfig.builder().addAllServers(uris).sslConfiguration(SSL_CONFIGURATION).build(),
                serviceInterface,
                TestProxyUtils.AUXILIARY_REMOTING_PARAMETERS));
    }

    public List<String> getServerUris() {
        return servers.stream()
                .map(this::getServerUri)
                .collect(Collectors.toList());
    }

    public List<String> getServerUrisForClient(String client) {
        return getServerUris().stream()
                .map(uri -> uri + "/" + client)
                .collect(Collectors.toList());
    }

    private String getServerUri(TimeLockServerHolder server) {
        return baseUri + ":" + server.getTimelockPort();
    }

    private String getServerUriForClient(String client, TimeLockServerHolder server) {
        return getServerUriForClient(client, getServerUri(server));
    }

    private String getServerUriForClient(String client, String uri) {
        return uri + "/" + client;
    }

}
