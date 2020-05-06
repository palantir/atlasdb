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

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.conjure.java.config.ssl.TrustContext;

/**
 * Defines how HTTP clients are created.
 *
 * Proxies returned here should already be instrumented, if the user desires. Clients should not add direct
 * instrumentation to proxies returned by this class via {@link com.palantir.atlasdb.util.AtlasDbMetrics}.
 */
public interface HttpClientCreationStrategy extends Closeable {
    <T> T createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);

    <T> T createProxyWithFailover(
            MetricsManager metricsManager,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);

    <T> T createLiveReloadingProxyWithFailover(
            MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters clientParameters);

    <T> T createProxyWithQuickFailoverForTesting(
            MetricsManager metricsManager,
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);
}
