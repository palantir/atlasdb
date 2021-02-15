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

import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.conjure.java.config.ssl.TrustContext;
import java.util.Optional;
import java.util.function.Supplier;
import org.immutables.value.Value;

public interface TargetFactory {
    <T> InstanceAndVersion<T> createProxy(
            Optional<TrustContext> trustContext, String uri, Class<T> type, AuxiliaryRemotingParameters parameters);

    /**
     * Creates a proxy that performs failover - that is, if the client encounters problems talking to a node of the
     * remote service, it may retry on other nodes in the specified {@link ServerListConfig}.
     *
     * Note that because failover by definition involves retrying on other nodes, the value of
     * {@link AuxiliaryRemotingParameters#shouldRetry()} is ignored; we will always retry in this case.
     */
    <T> InstanceAndVersion<T> createProxyWithFailover(
            ServerListConfig serverListConfig, Class<T> type, AuxiliaryRemotingParameters parameters);

    /**
     * Creates a live reloading proxy that performs failover.
     *
     * Proxies created by this method are able to detect changes to the {@code serverListConfigSupplier} at runtime.
     * However, there are no guarantees beyond eventual consistency here.
     *
     * This method should not be used if the {@link ServerListConfig} is not expected to change, as it may incur
     * unnecessary resource load from attempting to establish consistency. Please use
     * {@link #createProxyWithFailover(ServerListConfig, Class, AuxiliaryRemotingParameters)} in that case.
     */
    <T> InstanceAndVersion<T> createLiveReloadingProxyWithFailover(
            Supplier<ServerListConfig> serverListConfigSupplier, Class<T> type, AuxiliaryRemotingParameters parameters);

    @Value.Immutable
    interface InstanceAndVersion<T> {
        @Value.Parameter
        T instance();

        @Value.Parameter
        String version();
    }
}
