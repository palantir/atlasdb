/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import java.util.Optional;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.config.ssl.TrustContext;

public enum LegacyStaticHttpClientCreationStrategy implements HttpClientCreationStrategy {
    INSTANCE;

    @Override
    public <T> T createProxy(Optional<TrustContext> trustContext, String uri, Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return AtlasDbHttpClients.createProxy(trustContext, uri, type, parameters);
    }

    @Override
    public <T> T createProxyWithFailover(MetricsManager metricsManager, ServerListConfig serverListConfig,
            Class<T> type, AuxiliaryRemotingParameters parameters) {
        return AtlasDbHttpClients.createProxyWithFailover(metricsManager, serverListConfig, type, parameters);
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier, Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        return AtlasDbHttpClients.createLiveReloadingProxyWithFailover(
                metricsManager, serverListConfigSupplier, type, clientParameters);
    }

    @Override
    public <T> T createProxyWithQuickFailoverForTesting(MetricsManager metricsManager,
            ServerListConfig serverListConfig, Class<T> type, AuxiliaryRemotingParameters parameters) {
        return AtlasDbHttpClients.createProxyWithQuickFailoverForTesting(
                metricsManager, serverListConfig, type, parameters);
    }

    @Override
    public void close() {
        // No resources needed
    }
}
