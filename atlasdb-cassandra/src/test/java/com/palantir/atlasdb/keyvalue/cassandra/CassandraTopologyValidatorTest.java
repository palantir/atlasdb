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
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.ConsistentClusterTopologies;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTopologyValidator.NodesAndSharedTopology;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.apache.thrift.TApplicationException;
import org.junit.jupiter.api.Test;

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
    private static final Set<String> DIFFERENT_UUIDS = ImmutableSet.of("uuid4", "uuid5", "uuid6");
    private static final Set<String> MAYBE_SAME_AS_FIRST_CLUSTER = ImmutableSet.of("uuid7", "uuid8", "uuid1");

    private static final Set<String> ALL_HOSTS = Sets.union(NEW_HOSTS, OLD_HOSTS);

    private static final HostIdResult DEFAULT_RESULT = HostIdResult.success(UUIDS);

    private final CassandraTopologyValidationMetrics metrics =
            CassandraTopologyValidationMetrics.of(new DefaultTaggedMetricRegistry());
    private final AtomicReference<Set<String>> configServers = new AtomicReference<>();
    private final CassandraTopologyValidator validator =
            spy(CassandraTopologyValidator.createForTests(metrics, configServers::get));

    @Test
    public void retriesUntilNoNewHostsReturned() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
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
        configServers.set(ImmutableSet.of());
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        setHostIds(allHosts.values(), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void returnsNewHostsWhenOnlyNewServersAndOneNodeDisagrees() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        setHostIds(
                filterContainers(allHosts, server -> !server.equals(NEW_HOST_ONE)),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, NEW_HOST_ONE::equals),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid4")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void returnsEmptyWhenOnlyNewServersAndOneNodeDisagreesWithPlausibleEvolution() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        setHostIds(
                filterContainers(allHosts, server -> !server.equals(NEW_HOST_ONE)),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, NEW_HOST_ONE::equals),
                HostIdResult.success(ImmutableSet.of("uuid2", "uuid3")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void returnsEmptyWhenOnlyNewServersAndAllDisagreeWithPossiblyDisjointPlausibleEvolutions() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        setHostIds(
                filterContainers(allHosts, NEW_HOST_ONE::equals),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, NEW_HOST_TWO::equals),
                HostIdResult.success(ImmutableSet.of("uuid2", "uuid3")));
        setHostIds(
                filterContainers(allHosts, NEW_HOST_THREE::equals),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid4")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void returnsNewHostsWhenNewServersMismatchOldServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
        Set<CassandraServer> newServers = filterServers(allHosts, NEW_HOSTS::contains);
        setHostIds(
                filterContainers(allHosts, NEW_HOSTS::contains),
                HostIdResult.success(ImmutableSet.of("uuid1", "uuid2")));
        setHostIds(
                filterContainers(allHosts, OLD_HOSTS::contains),
                HostIdResult.success(ImmutableSet.of("uuid3", "uuid4")));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(newServers), allHosts))
                .containsExactlyElementsOf(newServers);
    }

    @Test
    public void validateNewlyAddedHostsReturnsMismatchingNewHostsWithoutPlausibleEvolution() {
        Predicate<String> badHostFilter = NEW_HOST_ONE::equals;
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
        setHostIds(filterContainers(allHosts, badHostFilter.negate()), DEFAULT_RESULT);
        setHostIds(filterContainers(allHosts, badHostFilter), HostIdResult.success(DIFFERENT_UUIDS));
        Set<CassandraServer> newServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<CassandraServer> badNewHosts = filterServers(allHosts, badHostFilter);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(newServers), allHosts))
                .containsExactlyElementsOf(badNewHosts);
    }

    @Test
    public void validateNewlyAddedHostsDoesNotReturnMismatchingNewHostsWithPlausibleEvolution() {
        Predicate<String> badHostFilter = NEW_HOST_ONE::equals;
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
        setHostIds(filterContainers(allHosts, badHostFilter.negate()), DEFAULT_RESULT);
        setHostIds(filterContainers(allHosts, badHostFilter), HostIdResult.success(ImmutableSet.of("uuid1", "uuid4")));
        Set<CassandraServer> newServers = filterServers(allHosts, NEW_HOSTS::contains);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(newServers), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfNoHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        setHostIds(allHosts.values(), HostIdResult.softFailure());
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfOneHostHasEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(NEW_HOSTS);
        Set<String> hostWithEndpoint = Set.of(NEW_HOST_ONE);
        setHostIds(
                filterContainers(allHosts, server -> !hostWithEndpoint.contains(server)), HostIdResult.softFailure());
        setHostIds(filterContainers(allHosts, hostWithEndpoint::contains), DEFAULT_RESULT);
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddsAllHostsIfEndpointExistingHostIdsMatch() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
    public void validateNewlyAddedHostsNoNewHostsAddedIfNeitherOldNorNewHostsHaveQuorumAndNoPreviousResultExists() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
        Set<CassandraServer> newCassandraServers = filterServers(allHosts, NEW_HOSTS::contains);
        Set<String> hostsOffline = ImmutableSet.of(OLD_HOST_ONE, OLD_HOST_TWO, NEW_HOST_ONE, NEW_HOST_TWO);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("no new hosts added if neither old nor new hosts have quorum")
                .containsExactlyElementsOf(newCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsNewHostsAddedIfTheyAgreeWithOldHostsOnPreviousTopology() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
    public void
            validateNewlyAddedHostsNewHostsAddedIfTheyPresentAPlausibleEvolutionOfPreviousTopologyAndCurrentServersExist() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
                HostIdResult.success(MAYBE_SAME_AS_FIRST_CLUSTER));

        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("accepts a quorum from new hosts if it is different from the previous topology but a plausible"
                        + " evolution")
                .isEmpty();
    }

    @Test
    public void acceptsNewHostsIfNoConsensusOnNewHostsWhenNoQuorumAndCurrentServersExist() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
                .as("accepts servers with plausible evolution when no quorum from all hosts and no quorum on new hosts")
                .isEmpty();
    }

    @Test
    public void returnsAllNewHostsIfConsensusOnNewHostsDiffersFromPreviousWhenNoQuorumAndCurrentServersExist() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
                .containsExactlyInAnyOrderElementsOf(newCassandraServers);
    }

    @Test
    public void newlyAddedHostsAcceptedIfAgreedWithOldHostsOnPreviousTopologyWhenNoQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
    public void newlyAddedHostsAddedIfPresentingPlausiblyEvolvedPreviousTopologyWhenNoQuorum() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
                HostIdResult.success(MAYBE_SAME_AS_FIRST_CLUSTER));

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(newCassandraServers), allHosts))
                .as("accepts quorum from new hosts if they have at least one host IDs from the old topology")
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsAddedIfAgreedWithOldTopologyWhenNoQuorumAndNoCurrentServersWithRingOrigin() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
        configServers.set(NEW_HOSTS);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(allCassandraServers), allHosts))
                .as("accepts quorum from config hosts if they have the same host IDs as old topology when no current"
                        + " servers")
                .containsExactlyInAnyOrderElementsOf(oldCassandraServers);
    }

    @Test
    public void validateNewlyAddedHostsAddedIfAgreedWithOldTopologyWhenNoQuorumAndNoCurrentServersWithConfigOrigin() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
        configServers.set(NEW_HOSTS);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(allCassandraServers, oldCassandraServers)),
                        allHosts))
                .as("accepts quorum from config hosts if they have the same host IDs as old topology when no current"
                        + " servers")
                .containsExactlyInAnyOrderElementsOf(oldCassandraServers);
    }

    @Test
    public void acceptsNewHostsIfNoConsensusOnConfigHostsWhenNoQuorumAndNoCurrentServers() {
        Iterator<String> uuidIterator = UUIDS.iterator();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
        configServers.set(NEW_HOSTS);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(allCassandraServers, oldCassandraServers)),
                        allHosts))
                .as("accepts new servers with a plausible evolution when no quorum from all hosts and no quorum on"
                        + " available hosts")
                .containsExactlyInAnyOrderElementsOf(oldCassandraServers);
    }

    @Test
    public void returnsAllNewHostsIfConsensusOnConfigHostsDiffersFromPreviousWhenNoQuorumAndNoCurrentServers() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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
                .containsExactlyInAnyOrderElementsOf(allCassandraServers);
    }

    @Test
    public void acceptsNewHostsIfConsensusOnConfigHostsIsEvolutionOfPreviousNoQuorumAcceptAndNoCurrentServers() {
        // We split hosts into three "generations": old, new and additional. The "original hosts" refer to the old
        // and new hosts, but not the additional ones.
        Map<CassandraServer, CassandraClientPoolingContainer> originalHosts = initialiseHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(originalHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> originalCassandraServers = originalHosts.keySet();

        setHostIds(filterContainers(originalHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(originalHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(
                filterContainers(originalHosts, server -> !hostsOffline.contains(server)),
                HostIdResult.success(MAYBE_SAME_AS_FIRST_CLUSTER));

        configServers.set(NEW_HOSTS);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(
                                oldCassandraServers, Sets.difference(originalCassandraServers, oldCassandraServers)),
                        originalHosts))
                .as("accepts new hosts because their host IDs were a plausible evolution of that of the old hosts")
                .containsExactlyInAnyOrderElementsOf(oldCassandraServers);

        Map<CassandraServer, CassandraClientPoolingContainer> additionalHosts =
                initialiseHosts(ImmutableSet.of("additional_host_1", "additional_host_2", "additional_host_3"));
        setHostIds(additionalHosts.values(), HostIdResult.success(ImmutableSet.of("uuid7", "uuid9")));
        setHostIds(originalHosts.values(), HostIdResult.hardFailure());

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        configServers.set(additionalHosts.keySet().stream()
                .map(CassandraServer::cassandraHostName)
                .collect(Collectors.toSet()));
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        splitHostOriginBetweenLastKnownAndConfig(originalCassandraServers, additionalHosts.keySet()),
                        ImmutableMap.<CassandraServer, CassandraClientPoolingContainer>builder()
                                .putAll(originalHosts)
                                .putAll(additionalHosts)
                                .buildOrThrow()))
                .as("accepts additional hosts because their host IDs were a plausible evolution of that of the new"
                        + " hosts (even if they were not that of the old hosts directly)")
                .containsExactlyInAnyOrderElementsOf(originalCassandraServers);
    }

    @Test
    public void acceptsNewHostsIfConsensusOnTokenRingDiscoveredHostsIsEvolutionOfPreviousNoQuorumAccept() {
        // We split hosts into three "generations": old, new and additional. The "original hosts" refer to the old
        // and new hosts, but not the additional ones.
        Map<CassandraServer, CassandraClientPoolingContainer> originalHosts = initialiseHosts(ALL_HOSTS);
        Map<CassandraServer, CassandraClientPoolingContainer> oldHosts =
                filterServerToContainerMap(originalHosts, OLD_HOSTS::contains);
        Set<CassandraServer> oldCassandraServers = oldHosts.keySet();
        Set<CassandraServer> originalCassandraServers = originalHosts.keySet();

        setHostIds(filterContainers(originalHosts, OLD_HOSTS::contains), HostIdResult.success(UUIDS));
        validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(oldCassandraServers), oldHosts);

        Set<String> hostsOffline = OLD_HOSTS;
        setHostIds(filterContainers(originalHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(
                filterContainers(originalHosts, server -> !hostsOffline.contains(server)),
                HostIdResult.success(MAYBE_SAME_AS_FIRST_CLUSTER));

        configServers.set(NEW_HOSTS);
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(Sets.difference(originalCassandraServers, oldCassandraServers)),
                        originalHosts))
                .as("accepts new hosts because their host IDs were a plausible evolution of that of the old hosts")
                .isEmpty();

        Map<CassandraServer, CassandraClientPoolingContainer> additionalHosts =
                initialiseHosts(ImmutableSet.of("additional_host_1", "additional_host_2", "additional_host_3"));
        setHostIds(additionalHosts.values(), HostIdResult.success(ImmutableSet.of("uuid7", "uuid9")));
        setHostIds(originalHosts.values(), HostIdResult.hardFailure());

        assertThat(hostsOffline).hasSizeGreaterThanOrEqualTo((ALL_HOSTS.size() + 1) / 2);
        configServers.set(additionalHosts.keySet().stream()
                .map(CassandraServer::cassandraHostName)
                .collect(Collectors.toSet()));
        assertThat(validator.getNewHostsWithInconsistentTopologies(
                        mapToTokenRangeOrigin(additionalHosts.keySet()),
                        ImmutableMap.<CassandraServer, CassandraClientPoolingContainer>builder()
                                .putAll(originalHosts)
                                .putAll(additionalHosts)
                                .buildOrThrow()))
                .as("accepts additional hosts because their host IDs were a plausible evolution of that of the new"
                        + " hosts (even if they were not that of the old hosts directly)")
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHostsNoNewHostsAddedIfNewHostsDoNotHaveQuorumAndNoCurrentServers() {
        Set<String> hosts = ImmutableSet.<String>builder()
                .addAll(NEW_HOSTS)
                .add(OLD_HOST_ONE)
                .build();
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(hosts);
        Set<String> hostsOffline = ImmutableSet.of(NEW_HOST_ONE, NEW_HOST_TWO);
        setHostIds(filterContainers(allHosts, hostsOffline::contains), HostIdResult.hardFailure());
        setHostIds(filterContainers(allHosts, server -> !hostsOffline.contains(server)), HostIdResult.success(UUIDS));
        assertThat(validator.getNewHostsWithInconsistentTopologies(mapToTokenRangeOrigin(allHosts.keySet()), allHosts))
                .containsExactlyElementsOf(allHosts.keySet());
    }

    @Test
    public void validateNewlyAddedHostsAddsAllNewHostsIfCurrentServersDoNotHaveEndpoint() {
        Map<CassandraServer, CassandraClientPoolingContainer> allHosts = initialiseHosts(ALL_HOSTS);
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

    @Test
    public void consistentClusterTopologiesCombinesUnderlyingTopologies() {
        Set<CassandraServer> newServers = NEW_HOSTS.stream()
                .map(CassandraTopologyValidatorTest::createCassandraServer)
                .collect(Collectors.toSet());
        Set<CassandraServer> oldServers = OLD_HOSTS.stream()
                .map(CassandraTopologyValidatorTest::createCassandraServer)
                .collect(Collectors.toSet());
        ConsistentClusterTopologies topologies = ConsistentClusterTopologies.builder()
                .addNodesAndSharedTopologies(
                        NodesAndSharedTopology.builder()
                                .hostIds(UUIDS)
                                .addAllServersInConsensus(newServers)
                                .build(),
                        NodesAndSharedTopology.builder()
                                .hostIds(MAYBE_SAME_AS_FIRST_CLUSTER)
                                .addAllServersInConsensus(oldServers)
                                .build())
                .build();

        assertThat(topologies.hostIds()).containsExactlyInAnyOrderElementsOf(Sets.union(UUIDS, MAYBE_SAME_AS_FIRST_CLUSTER));
        assertThat(topologies.serversInConsensus()).containsExactlyInAnyOrderElementsOf(Sets.union(oldServers, newServers));
    }

    @Test
    public void mergedConsistentClusterTopologyShouldHaveCorrectIdsAndServers() {
        ConsistentClusterTopologies original = ConsistentClusterTopologies.builder()
                .addNodesAndSharedTopologies(NodesAndSharedTopology.builder()
                        .addServersInConsensus(createCassandraServer("apple"))
                        .addHostIds("one", "two", "three")
                        .build())
                .build();
        ConsistentClusterTopologies newTopologies = original.merge(ImmutableMap.of(
                createCassandraServer("banana"),
                NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("one", "four"))),
                createCassandraServer("cherry"),
                NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("one", "five")))));

        assertThat(newTopologies.serversInConsensus())
                .containsExactlyInAnyOrderElementsOf(Sets.union(original.serversInConsensus(), newTopologies.serversInConsensus()));
        assertThat(newTopologies.hostIds()).containsExactlyInAnyOrderElementsOf(Sets.union(original.hostIds(), newTopologies.hostIds()));
    }

    @Test
    public void throwsIfMergingConsistentClusterTopologiesThatDoNotOverlap() {
        ConsistentClusterTopologies existing = ConsistentClusterTopologies.builder()
                .addNodesAndSharedTopologies(NodesAndSharedTopology.builder()
                        .addServersInConsensus(createCassandraServer("apple"))
                        .addHostIds("one", "two", "three")
                        .build())
                .build();

        assertThatThrownBy(() -> existing.merge(ImmutableMap.of(
                        createCassandraServer("banana"),
                        NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("one", "four"))),
                        createCassandraServer("cherry"),
                        NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("five", "six"))))))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Should not merge topologies that do not share at least one host id.");

        assertThatThrownBy(() -> existing.merge(ImmutableMap.of(
                        createCassandraServer("banana"),
                        NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("four", "five"))),
                        createCassandraServer("cherry"),
                        NonSoftFailureHostIdResult.wrap(HostIdResult.success(ImmutableSet.of("five", "six"))))))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Should not merge topologies that do not share at least one host id.");
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

    private Map<CassandraServer, CassandraClientPoolingContainer> initialiseHosts(Set<String> allHostNames) {
        configServers.set(allHostNames);
        return setupHosts(allHostNames);
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
