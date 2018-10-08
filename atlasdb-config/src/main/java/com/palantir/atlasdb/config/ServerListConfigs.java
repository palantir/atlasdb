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

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class ServerListConfigs {
    private ServerListConfigs() {
        // utilities
    }

    public static ServerListConfig parseInstallAndRuntimeConfigs(TimeLockClientConfig installClientConfig,
            Supplier<Optional<TimeLockRuntimeConfig>> runtimeConfig,
            String namespace) {
        ServerListConfig nonNamespacedConfig = runtimeConfig.get()
                .map(TimeLockRuntimeConfig::serversList)
                .orElse(installClientConfig.serversList());
        return namespaceUris(nonNamespacedConfig, namespace);
    }

    public static ServerListConfig namespaceUris(ServerListConfig config, String namespace) {
        Set<String> serversWithNamespaces = config
                .servers()
                .stream()
                .map(serverAddress -> serverAddress.replaceAll("/$", "") + "/" + namespace)
                .collect(Collectors.toSet());
        return ImmutableServerListConfig.builder()
                .from(config)
                .servers(serversWithNamespaces)
                .build();
    }
}
