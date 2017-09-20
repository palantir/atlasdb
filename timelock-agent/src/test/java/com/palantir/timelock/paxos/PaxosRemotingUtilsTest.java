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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.remoting.api.config.service.PartialServiceConfiguration;
import com.palantir.remoting.api.config.ssl.SslConfiguration;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.ImmutableClusterConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockInstallConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;

public class PaxosRemotingUtilsTest {
    private static final List<String> CLUSTER_URIS = ImmutableList.of("foo:1", "bar:2", "baz:3");

    private static final ClusterConfiguration NO_SSL_CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer("foo:1")
            .cluster(PartialServiceConfiguration.builder().addAllUris(CLUSTER_URIS).build())
            .build();
    private static final TimeLockInstallConfiguration NO_SSL_TIMELOCK = ImmutableTimeLockInstallConfiguration.builder()
            .paxos(mock(PaxosInstallConfiguration.class))
            .cluster(NO_SSL_CLUSTER)
            .build();

    private static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(Paths.get("dev", "null"));
    private static final ClusterConfiguration SSL_CLUSTER = ImmutableClusterConfiguration.builder()
            .localServer("foo:1")
            .cluster(PartialServiceConfiguration.builder()
                    .addAllUris(CLUSTER_URIS)
                    .security(SSL_CONFIGURATION)
                    .build())
            .build();
    private static final TimeLockInstallConfiguration SSL_TIMELOCK = ImmutableTimeLockInstallConfiguration.builder()
            .paxos(mock(PaxosInstallConfiguration.class))
            .cluster(SSL_CLUSTER)
            .build();

    @Test
    public void quorumOfOneNodeIsOne() {
        verifyQuorumSize(1, 1);
    }

    @Test
    public void quorumsOfMultipleNodesAreMajorities() {
        verifyQuorumSize(2, 2);
        verifyQuorumSize(3, 2);
        verifyQuorumSize(5, 3);
        verifyQuorumSize(7, 4);
        verifyQuorumSize(12, 7);
    }

    @Test
    public void largeQuorumsAreCorrect() {
        int halfOfLargeCluster = 337;
        verifyQuorumSize(halfOfLargeCluster * 2, halfOfLargeCluster + 1);
        verifyQuorumSize(halfOfLargeCluster * 2 + 1, halfOfLargeCluster + 1);
    }

    private static void verifyQuorumSize(int nodes, int expected) {
        List<PaxosAcceptor> acceptorList = Lists.newArrayList();
        for (int i = 0; i < nodes; i++) {
            acceptorList.add(null);
        }
        assertEquals(expected, PaxosRemotingUtils.getQuorumSize(acceptorList));
    }

    @Test
    public void canGetRemoteServerPaths() {
        // foo should not be present, because it is the local server
        assertThat(PaxosRemotingUtils.getRemoteServerPaths(SSL_TIMELOCK)).containsExactlyInAnyOrder(
                "https://bar:2",
                "https://baz:3");
    }

    @Test
    public void canGetClusterAddresses() {
        assertThat(PaxosRemotingUtils.getClusterAddresses(SSL_TIMELOCK)).containsExactlyInAnyOrder(
                "foo:1",
                "bar:2",
                "baz:3");
    }

    @Test
    public void canGetRemoteServerAddresses() {
        assertThat(PaxosRemotingUtils.getRemoteServerAddresses(SSL_TIMELOCK)).containsExactlyInAnyOrder(
                "bar:2",
                "baz:3");
    }

    @Test
    public void canGetClusterConfiguration() {
        assertThat(PaxosRemotingUtils.getClusterConfiguration(SSL_TIMELOCK)).isEqualTo(SSL_CLUSTER);
        assertThat(PaxosRemotingUtils.getClusterConfiguration(NO_SSL_TIMELOCK)).isEqualTo(NO_SSL_CLUSTER);
    }

    @Test
    public void canGetSslConfiguration() {
        assertThat(PaxosRemotingUtils.getSslConfigurationOptional(SSL_TIMELOCK))
                .isEqualTo(Optional.of(SSL_CONFIGURATION));
        assertThat(PaxosRemotingUtils.getSslConfigurationOptional(NO_SSL_TIMELOCK)).isEqualTo(Optional.empty());
    }

    @Test
    public void addProtocolAddsHttpIfSslNotPresent() {
        assertThat(PaxosRemotingUtils.addProtocol(NO_SSL_TIMELOCK, "atlasdb:1234")).isEqualTo("http://atlasdb:1234");
    }

    @Test
    public void addProtocolAddsHttpsIfSslPresent() {
        assertThat(PaxosRemotingUtils.addProtocol(SSL_TIMELOCK, "atlasdb:1234")).isEqualTo("https://atlasdb:1234");
    }

    @Test
    public void addProtocolsAddsHttpIfSslNotPresent() {
        assertThat(PaxosRemotingUtils.addProtocols(NO_SSL_TIMELOCK, ImmutableSet.of("foo:1", "bar:2")))
                .containsExactlyInAnyOrder("http://foo:1", "http://bar:2");
    }

    @Test
    public void addProtocolsAddsHttpsIfSslPresent() {
        assertThat(PaxosRemotingUtils.addProtocols(SSL_TIMELOCK, ImmutableSet.of("foo:1", "bar:2")))
                .containsExactlyInAnyOrder("https://foo:1", "https://bar:2");
    }

}
