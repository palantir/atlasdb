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
package com.palantir.atlasdb.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

public class ClusterConfigurationTest {
    private static final String ADDRESS_1 = "localhost:1";
    private static final String ADDRESS_2 = "localhost:2";
    private static final String ADDRESS_3 = "localhost:3";
    private static final String ADDRESS_4 = "localhost:4";

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

    @Test
    public void shouldThrowIfConfigurationContainsOneServerAndDisclaimerNotEnabled() {
        assertConfigurationWithFixedNumberOfServersIsInvalidWithoutDisclaimer(1);
    }

    @Test
    public void shouldThrowIfConfigurationContainsTwoServersAndDisclaimerNotEnabled() {
        assertConfigurationWithFixedNumberOfServersIsInvalidWithoutDisclaimer(2);
    }

    @Test
    public void shouldPermitConfigurationContainingThreeServersWithoutDisclaimer() {
        assertConfigurationWithFixedNumberOfServersIsValidWithoutDisclaimer(3);
    }

    @Test
    public void shouldPermitConfigurationContainingMoreThanThreeServersWithoutDisclaimer() {
        assertConfigurationWithFixedNumberOfServersIsValidWithoutDisclaimer(4);
        assertConfigurationWithFixedNumberOfServersIsValidWithoutDisclaimer(77);
    }

    @Test
    public void shouldPermitConfigurationContainingOneServerIfDisclaimerSpecified() {
        assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(1);
    }

    @Test
    public void shouldPermitConfigurationContainingTwoServersIfDisclaimerSpecified() {
        assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(2);
    }

    @Test
    public void shouldPermitConfigurationContainingThreeOrMoreServersEvenIfDisclaimerSpecified() {
        assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(3);
        assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(314);
    }

    private void assertAddressValid(String address) {
        ClusterConfiguration.checkHostPortString(address);
    }

    private void assertAddressNotValid(String address) {
        assertThatThrownBy(() -> ClusterConfiguration.checkHostPortString(address))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private void assertConfigurationWithFixedNumberOfServersIsValidWithoutDisclaimer(int numServers) {
        ClusterConfiguration configuration = createClusterConfiguration(numServers, false);
        assertThat(configuration).isNotNull();
    }

    private void assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(int numServers) {
        ClusterConfiguration configuration = createClusterConfiguration(numServers, true);
        assertThat(configuration).isNotNull();
    }

    private ClusterConfiguration createClusterConfiguration(int numServers, boolean disclaimer) {
        List<String> servers = IntStream.range(0, numServers)
                .mapToObj(index -> "server" + index + ":42")
                .collect(Collectors.toList());
        return ImmutableClusterConfiguration.builder()
                .addAllServers(servers)
                .localServer(servers.get(0))
                .enableNonstandardAndPossiblyDangerousTopology(disclaimer)
                .build();
    }

    private void assertConfigurationWithFixedNumberOfServersIsInvalidWithoutDisclaimer(int numServers) {
        assertThatThrownBy(() -> createClusterConfiguration(numServers, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a standard configuration!")
                .hasMessageContaining("IRRECOVERABLY COMPROMISED");
    }
}
