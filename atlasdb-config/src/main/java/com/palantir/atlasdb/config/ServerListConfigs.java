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
package com.palantir.atlasdb.config;

import com.palantir.logsafe.Preconditions;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class ServerListConfigs {
    private ServerListConfigs() {
        // utilities
    }

    public static Refreshable<ServerListConfig> getTimeLockServersFromAtlasDbConfig(
            AtlasDbConfig config, Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig) {
        Refreshable<Optional<TimeLockRuntimeConfig>> timelockRuntimeConfig =
                Refreshable.only(atlasDbRuntimeConfig.flatMap(AtlasDbRuntimeConfig::timelockRuntime));
        return getServerListConfigSupplierForTimeLock(config, timelockRuntimeConfig);
    }

    public static Refreshable<ServerListConfig> getTimeLockServersFromAtlasDbConfig(
            AtlasDbConfig config, Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        Refreshable<Optional<TimeLockRuntimeConfig>> timelockRuntimeConfig =
                runtimeConfigSupplier.map(AtlasDbRuntimeConfig::timelockRuntime);
        return getServerListConfigSupplierForTimeLock(config, timelockRuntimeConfig);
    }

    private static Refreshable<ServerListConfig> getServerListConfigSupplierForTimeLock(
            AtlasDbConfig config, Refreshable<Optional<TimeLockRuntimeConfig>> timelockRuntimeConfig) {
        Preconditions.checkState(
                !config.remoteTimestampAndLockOrLeaderBlocksPresent(),
                "Cannot create raw services from timelock with another source of timestamps/locks configured!");
        TimeLockClientConfig clientConfig = config.timelock()
                .orElseGet(() -> ImmutableTimeLockClientConfig.builder().build());
        return ServerListConfigs.parseInstallAndRuntimeConfigs(clientConfig, timelockRuntimeConfig);
    }

    public static Refreshable<ServerListConfig> parseInstallAndRuntimeConfigs(
            TimeLockClientConfig installClientConfig, Refreshable<Optional<TimeLockRuntimeConfig>> runtimeConfig) {
        return runtimeConfig.map(
                config -> config.map(TimeLockRuntimeConfig::serversList).orElseGet(installClientConfig::serversList));
    }

    public static ServerListConfig namespaceUris(ServerListConfig config, String namespace) {
        Set<String> serversWithNamespaces = config.servers().stream()
                .map(serverAddress -> serverAddress.replaceAll("/$", "") + "/" + namespace)
                .collect(Collectors.toSet());
        return ImmutableServerListConfig.builder()
                .from(config)
                .servers(serversWithNamespaces)
                .build();
    }
}
