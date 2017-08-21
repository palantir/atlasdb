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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.Test;

import com.palantir.remoting2.config.service.ServiceConfiguration;

public class ClusterConfigurationTest {
    private static final String ADDRESS_1 = "localhost:1";
    private static final String ADDRESS_2 = "localhost:2";

    @Test
    public void shouldThrowIfLocalServerNotSpecified() {
        assertThatThrownBy(ImmutableClusterConfiguration.builder()::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowIfNoServersSpecified() {
        assertThatThrownBy(ImmutableClusterConfiguration.builder()
                .localServer(ADDRESS_1)
                ::build)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldThrowIfLocalServerNotInServers() {
        assertThatThrownBy(ImmutableClusterConfiguration.builder()
                .localServer(ADDRESS_1)
                .cluster(ServiceConfiguration.of(ADDRESS_2, Optional.empty()))
                ::build)
                .isInstanceOf(IllegalStateException.class);
    }
}
