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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class ClusterConfigurationTest {

    private static final String ADDRESS_1 = "localhost:1";
    private static final String ADDRESS_2 = "localhost:2";

    @Test
    public void shouldThrowIfLocalServerNotInServers() {
        ImmutableDefaultClusterConfiguration.Builder builder = ImmutableDefaultClusterConfiguration.builder()
                .localServer(ADDRESS_1)
                .enableNonstandardAndPossiblyDangerousTopology(true)
                .cluster(PartialServiceConfiguration.of(ImmutableList.of(ADDRESS_2), Optional.empty()));
        assertThatIllegalArgumentException().isThrownBy(builder::build);
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

    private static void assertConfigurationWithFixedNumberOfServersIsValidWithoutDisclaimer(int numServers) {
        ClusterConfiguration configuration = createClusterConfiguration(numServers, false);
        assertThat(configuration).isNotNull();
    }

    private static void assertConfigurationWithFixedNumberOfServersIsValidWithDisclaimer(int numServers) {
        ClusterConfiguration configuration = createClusterConfiguration(numServers, true);
        assertThat(configuration).isNotNull();
    }

    private static ClusterConfiguration createClusterConfiguration(int numServers, boolean disclaimer) {
        List<String> servers = IntStream.range(0, numServers)
                .mapToObj(index -> "server" + index + ":42")
                .collect(Collectors.toList());
        return ImmutableDefaultClusterConfiguration.builder()
                .cluster(PartialServiceConfiguration.of(servers, Optional.empty()))
                .localServer(servers.get(0))
                .enableNonstandardAndPossiblyDangerousTopology(disclaimer)
                .build();
    }

    private static void assertConfigurationWithFixedNumberOfServersIsInvalidWithoutDisclaimer(int numServers) {
        assertThatThrownBy(() -> createClusterConfiguration(numServers, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a standard configuration!")
                .hasMessageContaining("IRRECOVERABLY COMPROMISED");
    }
}
