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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

public class CassandraTopologyValidatorTest {

    private static final CassandraServer newHostOneCassandraServer =
            CassandraServer.of(InetSocketAddress.createUnresolved("1.1.1.1", 80));
    private static final String newHostOne = "new_host_one";
    private static final String newHostTwo = "new_host_two";
    private static final String newHostThree = "new_host_three";
    private static final Set<String> newHosts = ImmutableSet.of(newHostOne, newHostTwo, newHostThree);

    private static final String oldHostOne = "old_host_one";
    private static final String oldHostTwo = "old_host_two";
    private static final String oldHostThree = "old_host_three";
    private static final Set<String> oldHosts = ImmutableSet.of(oldHostOne, oldHostTwo, oldHostThree);
    private static final Set<String> uuids = ImmutableSet.of("uuid1", "uuid2", "uuid3");

    private static final HostIdResult DEFAULT_RESULT = HostIdResult.success(uuids);

    private CassandraTopologyValidator validator;
    private CassandraTopologyValidationMetrics metrics;

    @Before
    public void before() {
        metrics = mock(CassandraTopologyValidationMetrics.class);
        this.validator = spy(new CassandraTopologyValidator(metrics));
    }

    @Test
    public void retriesUntilNoNewHostsReturned() {
        Iterator<String> uuidIterator = uuids.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        EntryStream.of(allHosts)
                .values()
                .forEach(container -> setHostIds(
                        Set.of(container),
                        HostIdResult.success(Set.of(uuidIterator.next())),
                        HostIdResult.success(uuids)));
        assertThat(validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        allHosts.keySet(),
                        allHosts,
                        HumanReadableDuration.milliseconds(1),
                        HumanReadableDuration.seconds(20)))
                .isEmpty();
    }

    @Test
    public void retriesThrowsAndMarksFailureWhenNeverResolves() {
        Iterator<String> uuidIterator = uuids.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        EntryStream.of(allHosts)
                .values()
                .forEach(container -> setHostIds(Set.of(container), HostIdResult.success(Set.of(uuidIterator.next()))));
        assertThatThrownBy(() -> validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        allHosts.keySet(),
                        allHosts,
                        HumanReadableDuration.milliseconds(1),
                        HumanReadableDuration.milliseconds(1)))
                .isInstanceOf(SafeRuntimeException.class);
        verify(metrics, times(1)).markTopologyValidationFailure();
        verify(metrics, atLeastOnce()).recordTopologyValidationLatency(any());
    }

    @Test
    public void returnsEmptyWhenGivenNoServers() {
        assertThat(validator.getNewHostsWithInconsistentTopologies(ImmutableSet.of(), ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    public void throwsWhenAllHostsDoesNotContainNewHosts() {
        assertThatThrownBy(() -> validator.getNewHostsWithInconsistentTopologies(
                        ImmutableSet.of(createCassandraServer("foo")), ImmutableMap.of()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void returnsEmptyWhenOnlyNewServersAndAllMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(allHosts.values(), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void returnsNewHostsWhenOnlyNewServersAndOneMismatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(
                filterContainers(allHosts, server -> !server.equals(newHostOne)),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, newHostOne::equalsIgnoreCase),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void returnsNewHostsWhenNewServersMismatchOldServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newServers = filterServers(allHosts, newHosts::contains);
        setHostIds(
                filterContainers(allHosts, newHosts::contains),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, oldHosts::contains),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(newServers, allHosts))
                .containsExactlyElementsOf(newServers);
    }

    @Test
    public void validateNewlyAddedHostsReturnsOnlyMismatchingNewHosts() {
        Predicate<String> badHostFilter = newHostOne::equalsIgnoreCase;
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newServers = filterServers(allHosts, newHosts::contains);
        Set<CassandraServer> badNewHosts = filterServers(allHosts, badHostFilter);
        setHostIds(filterContainers(allHosts, badHostFilter.negate()), DEFAULT_RESULT);
        setHostIds(filterContainers(allHosts, badHostFilter), HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(newServers, allHosts))
                .containsExactlyElementsOf(badNewHosts);
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfNoHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        setHostIds(allHosts.values(), HostIdResult.softFailure());
        assertThat(validator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfOneHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        Set<String> hostWithEndpoint = Set.of(newHostOne);
        setHostIds(
                filterContainers(allHosts, server -> !hostWithEndpoint.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, hostWithEndpoint::contains), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfEndpointExistingHostIdsMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<String> hostsWithEndpoints = ImmutableSet.of(newHostOne, oldHostOne);
        setHostIds(
                filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, hostsWithEndpoints::contains), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        filterServers(allHosts, newHosts::contains), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsReturnsNewHostsUnlessEndpointExistAndDoNotMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<String> hostsWithEndpoints = ImmutableSet.of(newHostOne, oldHostOne);
        setHostIds(
                filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, newHostOne::equalsIgnoreCase), HostIdResult.success(Set.of("uuid")));
        setHostIds(filterContainers(allHosts, oldHostOne::equalsIgnoreCase), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        filterServers(allHosts, newHosts::contains), allHosts))
                .containsExactlyElementsOf(filterServers(allHosts, newHostOne::equalsIgnoreCase));
    }

    @Test
    public void validateNewlyAddedHostsNoNewHostsAddedIfOldHostsDoNotHaveQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(Sets.union(newHosts, oldHosts));
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, newHosts::contains);
        Set<String> hostsOffline = ImmutableSet.of(oldHostOne, oldHostTwo);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(uuids));
        assertThat(validator.getNewHostsWithInconsistentTopologies(newCassandraServers, allHosts))
                .containsExactlyElementsOf(newCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsNoNewHostsAddedIfNewHostsDoNotHaveQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(newHosts);
        Set<String> hostsOffline = ImmutableSet.of(newHostOne, newHostTwo);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(uuids));
        assertThat(validator.getNewHostsWithInconsistentTopologies(allHosts.keySet(), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void fetchHostIdsReturnsSoftFailureWhenException() throws Exception {
        CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);
        when(badContainer.getCassandraServer()).thenReturn(newHostOneCassandraServer);
        when(badContainer.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                .thenThrow(new RuntimeException());
        assertThat(validator.fetchHostIds(badContainer)).isEqualTo(HostIdResult.softFailure());
    }

    @Test
    public void fetchHostIdsReturnsHardFailureWhenNetworkErrors() throws Exception {
        CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);
        when(badContainer.getCassandraServer()).thenReturn(newHostOneCassandraServer);
        when(badContainer.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                .thenThrow(new TTransportException());
        assertThat(validator.fetchHostIds(badContainer)).isEqualTo(HostIdResult.hardFailure());
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

    private static void setHostIds(Collection<CassandraClientPoolingContainer> containers, HostIdResult... hostIds) {
        containers.forEach(container -> {
            try {
                when(container.<HostIdResult, Exception>runWithPooledResource(any()))
                        .thenReturn(hostIds[0], hostIds);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static Map<CassandraServer, CassandraClientPoolingContainer> setupHosts(Set<String> allHostNames) {
        return StreamEx.of(allHostNames)
                .map(CassandraTopologyValidatorTest::createCassandraServer)
                .mapToEntry(server -> mock(CassandraClientPoolingContainer.class))
                .toMap();
    }
}
