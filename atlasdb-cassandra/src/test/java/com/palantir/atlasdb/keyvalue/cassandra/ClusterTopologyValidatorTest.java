/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.junit.Test;

public class ClusterTopologyValidatorTest {

    private static final String newHostOne = "new_host_one";
    private static final String newHostTwo = "new_host_two";
    private static final String newHostThree = "new_host_three";
    private static final Set<String> newHosts = ImmutableSet.of(newHostOne, newHostTwo, newHostThree);

    private static final String oldHostOne = "old_host_one";
    private static final String oldHostTwo = "old_host_two";
    private static final String oldHostThree = "old_host_three";
    private static final Set<String> oldHosts = ImmutableSet.of(oldHostOne, oldHostTwo, oldHostThree);
    private static final Optional<Set<String>> uuids = Optional.of(ImmutableSet.of("uuid1", "uuid2", "uuid3"));

    @Test
    public void validateNewlyAddedHosts_returnsEmptyWhenGivenNoServers() {
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(ImmutableSet.of(), ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_throwsWhenAllHostsDoesNotContainNewHosts() {
        assertThatThrownBy(() -> ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(
                        ImmutableSet.of(createCassandraServer("foo")), ImmutableMap.of()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void validateNewlyAddedHosts_returnsEmpty_whenOnlyNewServers_andAllMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(allHosts.values(), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_returnsNewHosts_whenOnlyNewServers_andOneMismatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(
                filterContainers(allHosts, server -> !server.equals(newHostOne)),
                Optional.of(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, newHostOne::equalsIgnoreCase),
                Optional.of(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void validateNewlyAddedHosts_returnsNewHosts_whenNewServers_mismatchOldServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newServers = filterServers(allHosts, newHosts::contains);
        setHostIds(filterContainers(allHosts, newHosts::contains), Optional.of(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(filterContainers(allHosts, oldHosts::contains), Optional.of(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(newServers, allHosts))
                .containsExactlyElementsOf(newServers);
    }

    @Test
    public void validateNewlyAddedHosts_returnsOnlyMismatchingNewHosts() {
        Predicate<String> badHostFilter = newHostOne::equalsIgnoreCase;
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newServers = filterServers(allHosts, newHosts::contains);
        Set<CassandraServer> badNewHosts = filterServers(allHosts, badHostFilter);
        setHostIds(filterContainers(allHosts, badHostFilter.negate()), uuids);
        setHostIds(filterContainers(allHosts, badHostFilter), Optional.of(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(newServers, allHosts))
                .containsExactlyElementsOf(badNewHosts);
    }

    @Test
    public void validateNewlyAddedHosts_addsAllHosts_ifNoHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(allHosts.values(), Optional.empty());
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_addsAllHosts_ifOneHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        Set<String> hostWithEndpoint = Set.of(newHostOne);
        setHostIds(filterContainers(allHosts, server -> !hostWithEndpoint.contains(server)), Optional.empty());
        setHostIds(filterContainers(allHosts, hostWithEndpoint::contains), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_addsAllHosts_ifEndpointExistingHostIdsMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<String> hostsWithEndpoints = ImmutableSet.of(newHostOne, oldHostOne);
        setHostIds(filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), Optional.empty());
        setHostIds(filterContainers(allHosts, hostsWithEndpoints::contains), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(
                        filterServers(allHosts, newHosts::contains), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_addsNewHosts_unlessEndpointExistAndDoNotMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<String> hostsWithEndpoints = ImmutableSet.of(newHostOne, oldHostOne);
        setHostIds(filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), Optional.empty());
        setHostIds(filterContainers(allHosts, newHostOne::equalsIgnoreCase), Optional.of(Set.of("uuid")));
        setHostIds(filterContainers(allHosts, oldHostOne::equalsIgnoreCase), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(
                        filterServers(allHosts, newHosts::contains), allHosts))
                .containsExactlyElementsOf(filterServers(allHosts, newHostOne::equalsIgnoreCase));
    }

    @Test
    public void validateNewlyAddedHosts_noNewHostsAdded_ifOldHostsDoNotHaveQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, newHosts::contains);
        Set<String> hostsOffline = ImmutableSet.of(oldHostOne, oldHostTwo);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), Optional.of(ImmutableSet.of()));
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(newCassandraServers, allHosts))
                .containsExactlyElementsOf(newCassandraServers);
    }

    @Test
    public void validateNewlyAddedHosts_noNewHostsAdded_ifNewHostsDoNotHaveQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        Set<String> hostsOffline = ImmutableSet.of(newHostOne, newHostTwo);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), Optional.of(ImmutableSet.of()));
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), uuids);
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    public Set<CassandraClientPoolingContainer> filterContainers(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts, Predicate<String> filter) {
        return EntryStream.of(hosts)
                .mapKeys(CassandraServer::cassandraHostName)
                .filterKeys(filter)
                .values()
                .toSet();
    }

    public Set<CassandraServer> filterServers(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts, Predicate<String> filter) {
        return EntryStream.of(hosts)
                .filterKeys(cassandraServer -> filter.test(cassandraServer.cassandraHostName()))
                .keys()
                .toSet();
    }

    private static CassandraServer createCassandraServer(String hostname) {
        return CassandraServer.of(hostname, mock(InetSocketAddress.class));
    }

    private static void setHostIds(
            Collection<CassandraClientPoolingContainer> containers, Optional<Set<String>> hostIds) {
        containers.forEach(container -> {
            try {
                when(container.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                        .thenReturn(hostIds);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Map<CassandraServer, CassandraClientPoolingContainer> setupHosts(Set<String> allHostNames) {
        return StreamEx.of(allHostNames)
                .map(ClusterTopologyValidatorTest::createCassandraServer)
                .mapToEntry(server -> mock(CassandraClientPoolingContainer.class))
                .toMap();
    }
}
