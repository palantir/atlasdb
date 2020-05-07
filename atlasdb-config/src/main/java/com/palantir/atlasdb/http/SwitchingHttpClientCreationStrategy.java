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

import java.io.IOException;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.proxy.PredicateSwitchedProxy;
import com.palantir.conjure.java.config.ssl.TrustContext;

public class SwitchingHttpClientCreationStrategy implements HttpClientCreationStrategy {
    private final BooleanSupplier shouldUseFirst;
    private final HttpClientCreationStrategy first;
    private final HttpClientCreationStrategy second;

    public SwitchingHttpClientCreationStrategy(
            BooleanSupplier shouldUseFirst,
            HttpClientCreationStrategy first,
            HttpClientCreationStrategy second) {
        this.shouldUseFirst = shouldUseFirst;
        this.first = first;
        this.second = second;
    }

    @Override
    public <T> T createProxy(Optional<TrustContext> trustContext, String uri, Class<T> type,
            AuxiliaryRemotingParameters parameters) {
        return PredicateSwitchedProxy.newProxyInstance(
                first.createProxy(trustContext, uri, type, parameters),
                second.createProxy(trustContext, uri, type, parameters),
                shouldUseFirst::getAsBoolean,
                type);
    }

    @Override
    public <T> T createProxyWithFailover(MetricsManager metricsManager, ServerListConfig serverListConfig,
            Class<T> type, AuxiliaryRemotingParameters parameters) {
        return null;
    }

    @Override
    public <T> T createLiveReloadingProxyWithFailover(MetricsManager metricsManager,
            Supplier<ServerListConfig> serverListConfigSupplier, Class<T> type,
            AuxiliaryRemotingParameters clientParameters) {
        return null;
    }

    @Override
    public <T> T createProxyWithQuickFailoverForTesting(MetricsManager metricsManager,
            ServerListConfig serverListConfig, Class<T> type, AuxiliaryRemotingParameters parameters) {
        return null;
    }

    @Override
    public void close() {
        first.close();
        second.close();
    }
}
