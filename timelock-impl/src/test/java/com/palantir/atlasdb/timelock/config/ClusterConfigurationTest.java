/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldThrowIfLocalServerNotInServers() {
        assertThatThrownBy(ImmutableClusterConfiguration.builder()
                .localServer(ADDRESS_1)
                .addServers(ADDRESS_2)
                ::build)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldAcceptValidHostPortString() {
        assertAddressValid(ADDRESS_1);
    }

    @Test
    public void shouldAcceptHostSpecifiedAsIpAddress() {
        assertAddressValid("1.2.3.4:1");
    }

    @Test
    public void shouldThrowIfServerHasNoPort() {
        assertAddressNotValid("localhost");
    }

    @Test
    public void shouldThrowIfServerHasInvalidPort() {
        assertAddressNotValid("localhost:1234567");
    }

    @Test
    public void shouldThrowIfStringIncludesProtocol() {
        assertAddressNotValid("http://palantir.com:8080");
    }

    @Test
    public void shouldThrowIfPortUnparseable() {
        assertAddressNotValid("localhost:foo");
    }

    private void assertAddressValid(String address) {
        ClusterConfiguration.checkHostPortString(address);
    }

    private void assertAddressNotValid(String address) {
        assertThatThrownBy(() -> ClusterConfiguration.checkHostPortString(address))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
