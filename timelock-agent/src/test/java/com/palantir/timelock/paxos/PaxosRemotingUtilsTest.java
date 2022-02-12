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
package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class PaxosRemotingUtilsTest {
    private static final ImmutableList<String> CLUSTER_URIS = ImmutableList.of("foo:1", "bar:2", "baz:3");

    private static final ClusterConfiguration NO_SSL_CLUSTER = ImmutableDefaultClusterConfiguration.builder()
            .localServer("foo:1")
            .cluster(PartialServiceConfiguration.builder()
                    .addAllUris(CLUSTER_URIS)
                    .build())
            .build();
    private static final SslConfiguration SSL_CONFIGURATION = SslConfiguration.of(Paths.get("dev", "null"));
    private static final ClusterConfiguration SSL_CLUSTER = ImmutableDefaultClusterConfiguration.builder()
            .localServer("foo:1")
            .cluster(PartialServiceConfiguration.builder()
                    .addAllUris(CLUSTER_URIS)
                    .security(SSL_CONFIGURATION)
                    .build())
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
        List<PaxosAcceptor> acceptorList = new ArrayList<>();
        for (int i = 0; i < nodes; i++) {
            acceptorList.add(null);
        }
        assertThat(PaxosRemotingUtils.getQuorumSize(acceptorList)).isEqualTo(expected);
    }

    @Test
    public void canGetRemoteServerPaths() {
        // foo should not be present, because it is the local server
        assertThat(PaxosRemotingUtils.getRemoteServerPaths(SSL_CLUSTER))
                .containsExactly("https://bar:2", "https://baz:3");
    }

    @Test
    public void canGetClusterAddresses() {
        assertThat(PaxosRemotingUtils.getClusterAddresses(SSL_CLUSTER)).containsExactly("foo:1", "bar:2", "baz:3");
    }

    @Test
    public void canGetRemoteServerAddresses() {
        assertThat(PaxosRemotingUtils.getRemoteServerAddresses(SSL_CLUSTER)).containsExactly("bar:2", "baz:3");
    }

    @Test
    public void canGetSslConfiguration() {
        assertThat(PaxosRemotingUtils.getSslConfigurationOptional(SSL_CLUSTER)).contains(SSL_CONFIGURATION);
        assertThat(PaxosRemotingUtils.getSslConfigurationOptional(NO_SSL_CLUSTER))
                .isNotPresent();
    }

    @Test
    public void addProtocolAddsHttpIfSslNotPresent() {
        assertThat(PaxosRemotingUtils.addProtocol(NO_SSL_CLUSTER, "atlasdb:1234"))
                .isEqualTo("http://atlasdb:1234");
    }

    @Test
    public void addProtocolAddsHttpsIfSslPresent() {
        assertThat(PaxosRemotingUtils.addProtocol(SSL_CLUSTER, "atlasdb:1234")).isEqualTo("https://atlasdb:1234");
    }

    @Test
    public void addProtocolsAddsHttpIfSslNotPresent() {
        assertThat(PaxosRemotingUtils.addProtocols(NO_SSL_CLUSTER, ImmutableList.of("foo:1", "bar:2")))
                .containsExactly("http://foo:1", "http://bar:2");
    }

    @Test
    public void addProtocolsAddsHttpsIfSslPresent() {
        assertThat(PaxosRemotingUtils.addProtocols(SSL_CLUSTER, ImmutableList.of("foo:1", "bar:2")))
                .containsExactly("https://foo:1", "https://bar:2");
    }

    @Test
    public void convertAddressToUrlCreatesComponentsCorrectly_NoSsl() throws MalformedURLException {
        assertThat(PaxosRemotingUtils.convertAddressToUrl(NO_SSL_CLUSTER, "foo:42/timelock/api/timelock"))
                .isEqualTo(new URL("http", "foo", 42, "/timelock/api/timelock"));
    }

    @Test
    public void convertAddressToUrlCreatesComponentsCorrectly_Ssl() throws MalformedURLException {
        assertThat(PaxosRemotingUtils.convertAddressToUrl(SSL_CLUSTER, "foo:42/api/bar/baz/bzzt"))
                .isEqualTo(new URL("https", "foo", 42, "/api/bar/baz/bzzt"));
    }
}
