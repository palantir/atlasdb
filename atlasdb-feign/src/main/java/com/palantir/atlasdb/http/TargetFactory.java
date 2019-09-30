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

package com.palantir.atlasdb.http;

import java.util.Optional;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.config.ssl.TrustContext;

public interface TargetFactory {
    <T> InstanceAndVersion<T> createProxy(
            Optional<TrustContext> trustContext,
            String uri,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);

    <T> InstanceAndVersion<T> createProxyWithFailover(
            ServerListConfig serverListConfig,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);

    <T> InstanceAndVersion<T> createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier,
            Class<T> type,
            AuxiliaryRemotingParameters parameters);

    @Value.Immutable
    interface InstanceAndVersion<T> {
        @Value.Parameter
        T instance();

        @Value.Parameter
        String version();
    }
}
