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
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.CassandraTopologyValidationMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.apache.thrift.TApplicationException;
import org.junit.Test;

public final class CassandraTopologyValidatorTest {

    private static final CassandraServer NEW_HOST_ONE_CASSANDRA_SERVER =
            CassandraServer.of(InetSocketAddress.createUnresolved("1.1.1.1", 80));
    private static final String NEW_HOST_ONE = "new_host_one";
    private static final String NEW_HOST_TWO = "new_host_two";
    private static final String NEW_HOST_THREE = "new_host_three";
    private static final Set<String> NEW_HOSTS = ImmutableSet.of(NEW_HOST_ONE, NEW_HOST_TWO, NEW_HOST_THREE);

    private static final String OLD_HOST_ONE = "old_host_one";
    private static final String OLD_HOST_TWO = "old_host_two";
    private static final String OLD_HOST_THREE = "old_host_three";
    private static final Set<String> OLD_HOSTS = ImmutableSet.of(OLD_HOST_ONE, OLD_HOST_TWO, OLD_HOST_THREE);
    private static final Set<String> UUIDS = ImmutableSet.of("uuid1", "uuid2", "uuid3");
    private static final Set<String> DIFFERENT_UUIDS = ImmutableSet.of("uuid4", "uuid5", "uuid3");

    private static final Set<String> ALL_HOSTS = Sets.union(NEW_HOSTS, OLD_HOSTS);

    private static final HostIdResult DEFAULT_RESULT = HostIdResult.success(UUIDS);

    private final CassandraTopologyValidationMetrics metrics =
            CassandraTopologyValidationMetrics.of(new DefaultTaggedMetricRegistry());
    private final CassandraTopologyValidator validator = spy(new CassandraTopologyValidator(metrics));

    @Test
    public void retriesUntilNoNewHostsReturned() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        EntryStream.of(allHosts)
                .values()
                .forEach(container -> setHostIds(
                        Set.of(container),
                        HostIdResult.success(Set.of(uuidIterator.next())),
                        HostIdResult.success(UUIDS)));
        assertThat(validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        mapToTokenRangeOrigin(allHosts.keySet()),
                        allHosts,
                        Duration.ofMillis(1),
                        Duration.ofSeconds(20)))
                .isEmpty();
    }

    @Test
    public void retriesMarksFailureWhenNeverResolves() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        EntryStream.of(allHosts)
                .values()
                .forEach(container -> setHostIds(Set.of(container), HostIdResult.success(Set.of(uuidIterator.next()))));
        assertThat(validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        mapToTokenRangeOrigin(allHosts.keySet()), allHosts, Duration.ofMillis(1), Duration.ofMillis(1)))
                .isNotEmpty();
        assertThat(metrics.validationFailures().getCount()).isEqualTo(1);
        assertThat(metrics.validationLatency().getCount()).isEqualTo(1);
    }

    @Test
    public void retriesAndReturnsFailingHosts() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        doReturn(allHosts.keySet()).when(validator).getNewHostsWithInconsistentTopologies(any(), any());
        assertThat(validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        mapToTokenRangeOrigin(allHosts.keySet()),
                        setupHosts(NEW_HOSTS),
                        Duration.ofMillis(1),
                        Duration.ofMillis(1)))
                .containsExactlyInAnyOrderElementsOf(allHosts.keySet());
    }

    @Test
    public void retriesAtLeastTwoTimes() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        doReturn(allHosts.keySet()).when(validator).getNewHostsWithInconsistentTopologies(any(), any());
        when(validator.getNewHostsWithInconsistentTopologies(any(), any())).thenReturn(allHosts.keySet());
        assertThat(validator.getNewHostsWithInconsistentTopologiesAndRetry(
                        mapToTokenRangeOrigin(allHosts.keySet()),
                        setupHosts(NEW_HOSTS),
                        Duration.ofMillis(1),
                        Duration.ofMillis(1)))
                .isNotEmpty();
        verify(validator, atLeast(2)).getNewHostsWithInconsistentTopologies(any(), any());
    }

    @Test
    public void returnsEmptyWhenGivenNoServers() {
        assertThat(validator.getNewHostsWithInconsistentTopologies(ImmutableMap.of(), ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    public void throwsWhenAllHostsDoesNotContainNewHosts() {
        assertThatThrownBy(() -> validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(ImmutableSet.of(createCassandraServer("foo"))), ImmutableMap.of()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void returnsEmptyWhenOnlyNewServersAndAllHaveSameHostIds() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        setHostIds(allHosts.values(), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void returnsNewHostsWhenOnlyNewServersAndOneMismatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        setHostIds(
                filterContainers(allHosts, server -> !server.equals(NEW_HOST_ONE)),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, NEW_HOST_ONE::equals),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void returnsNewHostsWhenNewServersMismatchOldServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Set<CassandraServer> newServers = filterServers(allHosts, NEW_HOSTS::contains);
        setHostIds(
                filterContainers(allHosts, NEW_HOSTS::contains),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, OLD_HOSTS::contains),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(newServers), allHosts))
                .containsExactlyElementsOf(newServers);
    }

    @Test
    public void validateNewlyAddedHostsReturnsOnlyMismatchingNewHosts() {
        Predicate<String> badHostFilter = NEW_HOST_ONE::equals;
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        setHostIds(filterContainers(allHosts, badHostFilter.negate()), DEFAULT_RESULT);
        setHostIds(filterContainers(allHosts, badHostFilter), HostIdResult.success(ImmutableSet.of("uuid3", "uuid2")));
        Set<CassandraServer> newServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<CassandraServer> badNewHosts = filterServers(allHosts, badHostFilter);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(newServers), allHosts))
                .containsExactlyElementsOf(badNewHosts);
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfNoHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        setHostIds(allHosts.values(), HostIdResult.softFailure());
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfOneHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(NEW_HOSTS);
        Set<String> hostWithEndpoint = Set.of(NEW_HOST_ONE);
        setHostIds(
                filterContainers(allHosts, server -> !hostWithEndpoint.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, hostWithEndpoint::contains), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfEndpointExistingHostIdsMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Set<String> hostsWithEndpoints = ImmutableSet.of(NEW_HOST_ONE, OLD_HOST_ONE);
        setHostIds(
                filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, hostsWithEndpoints::contains), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(filterServers(allHosts, NEW_HOSTS::contains)), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsReturnsNewHostsUnlessEndpointExistAndDoesMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Set<String> hostsWithEndpoints = ImmutableSet.of(NEW_HOST_ONE, OLD_HOST_ONE);
        setHostIds(
                filterContainers(allHosts, server -> !hostsWithEndpoints.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, NEW_HOST_ONE::equals), HostIdResult.success(Set.of("uuid")));
        setHostIds(filterContainers(allHosts, OLD_HOST_ONE::equals), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(filterServers(allHosts, NEW_HOSTS::contains)), allHosts))
                .containsExactlyElementsOf(filterServers(allHosts, NEW_HOST_ONE::equals));
    }

    @Test
    public void validateNewlyAddedHostsNoNewHostsAddedIfOldHostsDoNotHaveQuorumAndNoPreviousResultExists() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<String> hostsOffline = ImmutableSet.of(OLD_HOST_ONE, OLD_HOST_TWO);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("no new hosts added if old hosts do not have quorum")
                .containsExactlyElementsOf(newCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsNewHostsAddedIfTheyAgreeWithOldHostsOnPreviousTopology() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<String> hostsOffline = ImmutableSet.of(OLD_HOST_ONE);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));

        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("accepts quorum from new hosts if they have the same host IDs")
                .isEmpty();
    }

    @Test
    public void
            validateNewlyAddedHostsNewHostsNotAddedIfTheyDisagreeWithOldHostsOnPreviousTopologyAndCurrentServersExist() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts = EntryStream.of(allHosts)
                .filterKeys(key -> OLD_HOSTS.contains(key.cassandraHostName()))
                .toMap();
        Map<CassandraServer, CassandraClientPoolingContainer> newHosts = EntryStream.of(allHosts)
                .filterKeys(key -> NEW_HOSTS.contains(key.cassandraHostName()))
                .toMap();
        Set<CassandraServer> oldCassandraServers = filterServers(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<String> hostsOffline = ImmutableSet.of(OLD_HOST_ONE);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());

        setHostIds(filterContainers(newHosts, Predicate.not(hostsOffline::contains)), HostIdResult.success(UUIDS));
        setHostIds(
                filterContainers(oldHosts, Predicate.not(hostsOffline::contains)),
                HostIdResult.success(DIFFERENT_UUIDS));

        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("does not accept quorum from new hosts if they have different host IDs")
                .containsExactlyElementsOf(newCassandraServers);
    }

    @Test
    public void returnsAllNewHostsIfNoConsensusOnNewHostsWhenNoQuorumAndCurrentServersExist() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        filterContainers(allHosts, server -> !hostsOffline.contains(server))
                .forEach(container ->
                        setHostIds(ImmutableSet.of(container), HostIdResult.success(Set.of(uuidIterator.next()))));

        // This is _not_ supposed to be the standard quorum calculation ((n / 2) + 1), but instead the amount of
        // hosts that need to be offline to _prevent_ quorum
        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("rejects all servers when no quorum from all hosts and no quorum on new hosts")
                .isEqualTo(newCassandraServers);
    }

    @Test
    public void returnsAllNewHostsIfConsensusOnNewHostsDiffersFromPreviousWhenNoQuorumAndCurrentServersExist() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(
                filterContainers(allHosts, server -> !hostsOffline.contains(server)),
                HostIdResult.success(DIFFERENT_UUIDS));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("rejects all servers when no quorum from all hosts and no quorum on available hosts")
                .isEqualTo(newCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsAddedIfAgreedWithOldHostsOnPreviousTopologyWhenNoQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("accepts quorum from new hosts if they have the same host IDs as old topology")
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddedIfAgreedWithOldTopologyWhenNoQuorumAndNoCurrentServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> allCassandraServers = allHosts.keySet();

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(allCassandraServers, oldCassandraServers)),
                        allHosts))
                .as("accepts quorum from config hosts if they have the same host IDs as old topology when no current"
                        + " servers")
                .isEqualTo(oldCassandraServers);
    }

    @Test
    public void returnsAllNewHostsIfNoConsensusOnConfigHostsWhenNoQuorumAndNoCurrentServers() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> allCassandraServers = allHosts.keySet();

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        filterContainers(allHosts, server -> !hostsOffline.contains(server))
                .forEach(container ->
                        setHostIds(ImmutableSet.of(container), HostIdResult.success(Set.of(uuidIterator.next()))));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(allCassandraServers, oldCassandraServers)),
                        allHosts))
                .as("rejects all servers when no quorum from all hosts and no quorum on available hosts")
                .isEqualTo(allCassandraServers);
    }

    @Test
    public void returnsAllNewHostsIfConsensusOnConfigHostsDiffersFromPreviousWhenNoQuorumAndNoCurrentServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(allHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> allCassandraServers = allHosts.keySet();

        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(
                filterContainers(allHosts, server -> !hostsOffline.contains(server)),
                HostIdResult.success(DIFFERENT_UUIDS));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);

        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(allCassandraServers, oldCassandraServers)),
                        allHosts))
                .as("rejects all servers when no quorum from all hosts and no quorum on available hosts")
                .isEqualTo(allCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsNoNewHostsAddedIfNewHostsDoNotHaveQuorumAndNoCurrentServers() {
        Set<String> hosts = ImmutableSet.<String>builder()
                .addAll(NEW_HOSTS)
                .add(OLD_HOST_ONE)
                .build();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(hosts);
        Set<String> hostsOffline = ImmutableSet.of(NEW_HOST_ONE, NEW_HOST_TWO);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void validateNewlyAddedHostsAddsAllNewHostsIfCurrentServersDoNotHaveEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = setupHosts(ALL_HOSTS);
        setHostIds(filterContainers(allHosts, NEW_HOSTS::contains), HostIdResult.success(UUIDS));
        setHostIds(filterContainers(allHosts, OLD_HOSTS::contains), HostIdResult.softFailure());
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(filterServers(allHosts, NEW_HOSTS::contains)), allHosts))
                .isEmpty();
    }

    @Test
    public void fetchHostIdsReturnsHardFailureWhenException() throws Exception {
        CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);
        when(badContainer.getCassandraServer()).thenReturn(NEW_HOST_ONE_CASSANDRA_SERVER);
        when(badContainer.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                .thenThrow(new RuntimeException());
        assertThat(validator.fetchHostIds(badContainer)).isEqualTo(HostIdResult.hardFailure());
    }

    @Test
    public void fetchHostIdsReturnsSoftFailureWhenMethodDoesNotExist() throws Exception {
        CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);
        when(badContainer.getCassandraServer()).thenReturn(NEW_HOST_ONE_CASSANDRA_SERVER);
        when(badContainer.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                .thenThrow(new TApplicationException(TApplicationException.UNKNOWN_METHOD));
        assertThat(validator.fetchHostIds(badContainer)).isEqualTo(HostIdResult.softFailure());
    }

    @Test
    public void fetchHostIdsReturnsHardFailureWhenApplicationExceptionIsNotUnknownMethod() throws Exception {
        CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);
        when(badContainer.getCassandraServer()).thenReturn(NEW_HOST_ONE_CASSANDRA_SERVER);
        when(badContainer.<Optional<Set<String>>, Exception>runWithPooledResource(any()))
                .thenThrow(new TApplicationException(TApplicationException.WRONG_METHOD_NAME));
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

    public Map<CassandraServer, CassandraClientPoolingContainer> filterServerToContainerMap(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts, Predicate<String> filter) {
        return EntryStream.of(hosts)
                .filterKeys(cassandraServer -> filter.test(cassandraServer.cassandraHostName()))
                .toMap();
    }

    public Set<CassandraServer> filterServers(
            Map<CassandraServer, CassandraClientPoolingContainer> hosts, Predicate<String> filter) {
        return filterServerToContainerMap(hosts, filter).keySet();
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

    private static Map<CassandraServer, CassandraServerOrigin> splitHostOriginBetweenLastKnownAndConfig(
            Set<CassandraServer> lastKnownHosts, Set<CassandraServer> configHosts) {
        return ImmutableMap.<CassandraServer, CassandraServerOrigin>builder()
                .putAll(CassandraServerOrigin.mapAllServersToOrigin(lastKnownHosts, CassandraServerOrigin.LAST_KNOWN))
                .putAll(CassandraServerOrigin.mapAllServersToOrigin(configHosts, CassandraServerOrigin.CONFIG))
                .buildOrThrow();
    }

    private static Map<CassandraServer, CassandraServerOrigin> mapToTokenRangeOrigin(Set<CassandraServer> servers) {
        return CassandraServerOrigin.mapAllServersToOrigin(servers, CassandraServerOrigin.TOKEN_RANGE);
    }
}
