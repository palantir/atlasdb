/**
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

import io.atomix.catalyst.transport.Address;

public class TimeLockServerConfigurationTest {
    private static final Address ADDRESS = new Address("localhost:8700");
    private static final ClusterConfiguration CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer(new Address(ADDRESS))
            .addServers(ADDRESS)
            .build();
    private static final Set<String> CLIENTS = ImmutableSet.of("client1", "client2");
    private static final Set<String> INVALID_CLIENT_SET = ImmutableSet.of("client/client");
    private static final Set<String> EMPTY_CLIENT_SET = ImmutableSet.of("");
    private static final Set<String> MIXED_CLIENT_SET = ImmutableSet.of("client1", "foo", "b@r");

    @Test
    public void shouldAddDefaultConfigurationIfNotIncluded() {
        TimeLockServerConfiguration configuration = new TimeLockServerConfiguration(null, CLUSTER, CLIENTS);
        assertThat(configuration.atomix()).isEqualTo(ImmutableAtomixConfiguration.DEFAULT);
    }

    @Test
    public void shouldRequireAtLeastOneClient() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, ImmutableSet.of()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowIfClientHasInvalidName() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, INVALID_CLIENT_SET))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowIfClientHasEmptyName() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, EMPTY_CLIENT_SET))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowEvenIfValidClientsPresent() {
        assertThatThrownBy(() -> new TimeLockServerConfiguration(null, CLUSTER, MIXED_CLIENT_SET))
                .isInstanceOf(IllegalStateException.class);
    }
}
