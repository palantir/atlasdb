/*
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

public class TimeLockServerConfigurationTest {
    private static final String ADDRESS = "localhost:8701";
    private static final ClusterConfiguration CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer(ADDRESS)
            .addServers(ADDRESS)
            .build();
    private static final Set<String> CLIENTS = ImmutableSet.of("client1", "client2");

    @Test
    public void shouldAddDefaultConfigurationIfNotIncluded() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, null);
        assertThat(configuration.algorithm()).isEqualTo(ImmutableAtomixConfiguration.DEFAULT);
    }

    @Test
    public void shouldRequireAtLeastOneClient() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, ImmutableSet.of(), null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRejectClientsWithInvalidCharacters() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, ImmutableSet.of("/"), null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRejectClientsConflictingWithInternalClients() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(
                null,
                CLUSTER,
                ImmutableSet.of(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE),
                null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRejectClientsWithEmptyName() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, ImmutableSet.of(""), null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldHavePositiveNumberOfAvailableThreadsWhenUsingThreadPooling() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, true);
        assertThat(configuration.availableThreads()).isGreaterThan(0);
    }

    @Test
    public void shouldRequireUseThreadPoolingEnabledWhenCallingAvailableThreads() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, false);
        assertThatThrownBy(configuration::availableThreads).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldUseThreadPoolingIfTrue() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, true);
        assertThat(configuration.useThreadPooling()).isTrue();
    }

    @Test
    public void shouldNotUseThreadPoolingIfFalse() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, false);
        assertThat(configuration.useThreadPooling()).isFalse();
    }

    @Test
    public void shouldNotUseThreadPoolingIfNotIncluded() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS, null);
        assertThat(configuration.useThreadPooling()).isFalse();
    }
}
