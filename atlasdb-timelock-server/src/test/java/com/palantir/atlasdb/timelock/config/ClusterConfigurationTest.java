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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.atomix.catalyst.transport.Address;

public class ClusterConfigurationTest {

    private static final Address ADDRESS_1 = new Address("localhost:1");
    private static final Address ADDRESS_2 = new Address("localhost:2");

    @Test
    public void shouldThrowIfLocalServerNotSpecified() {
        assertThatThrownBy(
                () -> ImmutableClusterConfiguration.builder().build()
        ).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowIfNoServersSpecified() {
        assertThatThrownBy(
                () -> ImmutableClusterConfiguration.builder()
                        .localServer(ADDRESS_1)
                        .build()
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfLocalServerNotInServers() {
        assertThatThrownBy(
                () -> ImmutableClusterConfiguration.builder()
                        .localServer(ADDRESS_1)
                        .addServers(ADDRESS_2)
                        .build()
        ).isInstanceOf(IllegalArgumentException.class);
    }
}
