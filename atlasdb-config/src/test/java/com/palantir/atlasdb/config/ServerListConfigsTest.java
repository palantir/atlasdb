/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ServerListConfigsTest {
    private static final String CLIENT = "client";

    private static final ImmutableServerListConfig SERVERS_LIST_1 = ImmutableServerListConfig.builder()
            .addServers("one")
            .build();
    private static final ImmutableServerListConfig SERVERS_LIST_2 = ImmutableServerListConfig.builder()
            .addServers("one", "two")
            .build();
    private static final ImmutableServerListConfig SERVERS_LIST_3 = ImmutableServerListConfig.builder()
            .addServers("three/")
            .build();
    private static final ImmutableServerListConfig SERVERS_LIST_EMPTY = ImmutableServerListConfig.builder()
            .build();

    private static final TimeLockClientConfig INSTALL_CONFIG = ImmutableTimeLockClientConfig.builder()
            .serversList(SERVERS_LIST_1)
            .build();
    private static final TimeLockRuntimeConfig RUNTIME_CONFIG = ImmutableTimeLockRuntimeConfig.builder()
            .serversList(SERVERS_LIST_2)
            .build();

    @Test
    public void namespacingAddsClientNameCorrectly() {
        ServerListConfig namespacedServersList = ServerListConfigs.namespaceUris(SERVERS_LIST_1, CLIENT);
        assertThat(namespacedServersList.servers()).containsExactly("one/client");
    }

    @Test
    public void namespacingAddsClientNameToAllServers() {
        ServerListConfig namespacedServersList = ServerListConfigs.namespaceUris(SERVERS_LIST_2, CLIENT);
        assertThat(namespacedServersList.servers()).containsExactlyInAnyOrder("one/client", "two/client");
    }

    @Test
    public void namespacingCanDealWithTrailingSlash() {
        ServerListConfig namespacedServersList = ServerListConfigs.namespaceUris(SERVERS_LIST_3, CLIENT);
        assertThat(namespacedServersList.servers()).containsExactlyInAnyOrder("three/client");
    }

    @Test
    public void namespacingCanDealWithServerListConfigsWithZeroNodes() {
        ServerListConfig namespacedServersList = ServerListConfigs.namespaceUris(SERVERS_LIST_EMPTY, CLIENT);
        assertThat(namespacedServersList.servers()).isEmpty();
    }

    @Test
    public void prioritisesRuntimeConfigIfAvailable() {
        ServerListConfig resolvedConfig = ServerListConfigs.parseInstallAndRuntimeConfigs(
                INSTALL_CONFIG,
                () -> RUNTIME_CONFIG,
                CLIENT);
        assertThat(resolvedConfig.servers()).containsExactlyInAnyOrder("one/client", "two/client");
    }

    @Test
    public void prioritisesRuntimeConfigEvenIfThatHasNoClients() {
        ServerListConfig resolvedConfig = ServerListConfigs.parseInstallAndRuntimeConfigs(
                INSTALL_CONFIG,
                () -> ImmutableTimeLockRuntimeConfig.builder().build(),
                CLIENT);
        assertThat(resolvedConfig.servers()).isEmpty();
    }

    @Test
    public void fallsBackToInstallConfigIfRuntimeConfigNotAvailable() {
        ServerListConfig resolvedConfig = ServerListConfigs.parseInstallAndRuntimeConfigs(
                INSTALL_CONFIG,
                () -> ImmutableTimeLockRuntimeConfig.builder().build(),
                CLIENT);
        assertThat(resolvedConfig.servers()).containsExactlyInAnyOrder("one/client");
    }
}
