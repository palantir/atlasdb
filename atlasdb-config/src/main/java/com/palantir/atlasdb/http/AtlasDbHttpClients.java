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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.http.VersionSelectingClients.VersionSelectingConfig;
import com.palantir.atlasdb.http.v2.ConjureJavaRuntimeTargetFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.config.ssl.TrustContext;

public final class AtlasDbHttpClients {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbHttpClients.class);

    private AtlasDbHttpClients() {
        // Utility class
    }

    public static <T> T createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return Suppliers.memoizeWithExpiration(
                () -> ConjureJavaRuntimeTargetFactory.DEFAULT.createProxy(trustContext, uri, type, parameters)
                        .instance(),
                Duration.ofMinutes(30).toMinutes(),
                TimeUnit.MINUTES).get();

    }

    /**
     * Constructs an HTTP-invoking dynamic proxy for the specified type that will cycle through the list of supplied
     * endpoints after encountering an exception or connection failure, using the supplied SSL factory if it is
     * present. Also use the supplied the proxy selector to set the proxy on the clients if present.
     * <p>
     * Failover will continue to cycle through the supplied endpoint list indefinitely.
     */
    public static <T> T createProxyWithFailover(
            MetricsManager metricsManager,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return createExperimentallyWithFallback(
                metricsManager,
                () -> ConjureJavaRuntimeTargetFactory.DEFAULT.createProxyWithFailover(
                        serverListConfig, type, parameters),
                () -> AtlasDbFeignTargetFactory.DEFAULT.createProxyWithFailover(serverListConfig, type, parameters),
                type,
                parameters);
    }

    public static <T> T createLiveReloadingProxyWithFailover(
            MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        return createExperimentallyWithFallback(
                metricsManager,
                () -> ConjureJavaRuntimeTargetFactory.DEFAULT.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        type,
                        clientParameters),
                () -> AtlasDbFeignTargetFactory.DEFAULT.createLiveReloadingProxyWithFailover(
                        serverListConfigSupplier,
                        type,
                        clientParameters),
                type,
                clientParameters);
    }

    private static <T> T createExperimentallyWithFallback(
            MetricsManager metricsManager,
            Supplier<TargetFactory.InstanceAndVersion<T>> experimentalProxySupplier,
            Supplier<TargetFactory.InstanceAndVersion<T>> fallbackProxySupplier,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        TargetFactory.InstanceAndVersion<T> fallbackProxy = fallbackProxySupplier.get();
        try {
            return VersionSelectingClients.createVersionSelectingClientWithRefreshingNewClient(
                    metricsManager,
                    experimentalProxySupplier,
                    fallbackProxy,
                    VersionSelectingConfig.fromRemotingConfigSupplier(clientParameters.remotingClientConfig()),
                    type);
        } catch (Exception e) {
            log.warn("Error occurred in creating an experimental proxy. Possible causes include"
                    + " not running with SSL, which is deprecated and expected to be removed in a future release."
                    + " Creating a legacy client.", e);
            return fallbackProxy.instance();
        }
    }

    @VisibleForTesting
    static <T> T createProxyWithQuickFailoverForTesting(
            MetricsManager metricsManager,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        TargetFactory.InstanceAndVersion<T> instanceAndVersion =
                ConjureJavaRuntimeTargetFactory.DEFAULT.createProxyWithQuickFailoverForTesting(
                        serverListConfig, type, parameters);
        return VersionSelectingClients.instrumentWithClientVersionTag(
                metricsManager.getTaggedRegistry(), instanceAndVersion, type);
    }
}
