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
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.ImmutableServerListConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.client.config.ClientConfiguration;
import com.palantir.conjure.java.client.jaxrs.JaxRsClient;
import com.palantir.conjure.java.config.ssl.TrustContext;
import com.palantir.conjure.java.ext.refresh.Refreshable;
import com.palantir.conjure.java.okhttp.HostMetricsRegistry;

public final class AtlasDbFeignTargetFactory {
    private AtlasDbFeignTargetFactory() {
        // factory
    }

    public static <T> T createProxy(
            Collection<String> endpointUris,
            TrustContext trustContext,
            ClientOptions clientOptions,
            Optional<ProxySelector> proxy,
            Class<T> type,
            String userAgent) {
        ClientConfiguration clientConfiguration = clientOptions
                .fromStuff(ImmutableList.copyOf(endpointUris), proxy, trustContext);

        return JaxRsClient.create(type, createAgent(userAgent), new HostMetricsRegistry(), clientConfiguration);
    }

    static <T> T createLiveReloadingProxy(
            Supplier<ServerListConfig> serverListConfigSupplier,
            ClientOptions clientOptions,
            Class<T> type,
            String userAgent) {
        Supplier<ServerListConfig> nonEmptyServerList = () -> injectDummyServer(serverListConfigSupplier);
        Refreshable<ClientConfiguration> refreshableConfig = PollingRefreshable
                .createComposed(nonEmptyServerList, Duration.ofSeconds(5L), clientOptions::serverListToClient)
                .getRefreshable();

        return JaxRsClient.create(type, createAgent(userAgent), new HostMetricsRegistry(), refreshableConfig);
    }

    private static ServerListConfig injectDummyServer(Supplier<ServerListConfig> serverListConfigSupplier) {
        ServerListConfig originalConfig = serverListConfigSupplier.get();
        if (originalConfig.hasAtLeastOneServer()) {
            return originalConfig;
        }
        return ImmutableServerListConfig.builder().from(serverListConfigSupplier.get())
                .addServers("http://dummy")
                .build();
    }

    private static UserAgent createAgent(String userAgent) {
        return UserAgent.of(UserAgent.Agent.of(userAgent.replace('.', '-'), "0.0.0"));
    }
}
