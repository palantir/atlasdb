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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.VERSION_UNREACHABLE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public class CassandraKeyValueServicesSchemaConsensusTest {
    private CassandraKeyValueServiceConfig config = mock(CassandraKeyValueServiceConfig.class);
    private CassandraClient client = mock(CassandraClient.class);

    private static final String TABLE = "table";
    private static final String DC1 = "dc1";
    private static final String DC2 = "dc2";
    private static final String VERSION_1 = "v1";
    private static final String VERSION_2 = "v2";

    private Map<String, List<String>> versionToHostDc1 = new HashMap<>();
    private Map<String, List<String>> versionToHostDc2 = new HashMap<>();
    private Map<String, List<String>> allVersions = new HashMap<>();

    @Before
    public void initializeMocks() throws TException {
        when(config.schemaMutationTimeoutMillis()).thenReturn(0);
    }

    @Test
    public void waitSucceedsWhenAllHostsOnSameSchemaVersion() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 3);
        finalizeSchemaVersions();
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitThrowsWhenHostsDivergen() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 3);
        setNumberOfNodesForVersionPerDatacenter(VERSION_2, 0, 1);
        finalizeSchemaVersions();
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitSucceedsWhenQuorumIsAvailablePerDatacenterAndRestUnreachable() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 2);
        setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 1, 1);
        finalizeSchemaVersions();
        setRfPerDatacenter(3, 3);
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitSucceedsWhenQuorumIsAvailablePerDatacenterWithHigherRf() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 2);
        setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 2, 1);
        finalizeSchemaVersions();
        setRfPerDatacenter(5, 3);
        assertWaitForSchemaVersionsDoesNotThrow();
    }

    @Test
    public void waitThrowsWhenLessThanQuorumForSomeDatacenter() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 4);
        setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 1, 2);
        finalizeSchemaVersions();
        setRfPerDatacenter(3, 3);
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitThrowsWhenExactlyHalfOfTheNodesIsUnreachableForSomeTokenRange() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 2);
        setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 1, 1);
        finalizeSchemaVersions();
        setRfPerDatacenter(2, 2);
        assertWaitForSchemaVersionsThrows();
    }

    @Test
    public void waitSucceedsWhenQuorumIsAvailableForAllTokenRanges() throws TException {
        setNumberOfNodesForVersionPerDatacenter(VERSION_1, 2, 2);
        setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 3, 3);
        finalizeSchemaVersions();
        setRfPerDatacenter(3, 3);
        setOnlyTokenRangeToIncludeQuorumFromEachDc();
    }

    @Test
    public void waitWaitsForSchemaVersions() throws TException {
        when(client.describe_schema_versions())
                .thenAnswer(invocation -> {
                    setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 4);
                    setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 1, 2);
                    setRfPerDatacenter(3, 3);
                    return allVersions;
                })
                .thenAnswer(invocation1 -> {
                    cleanupVersions();
                    setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 3);
                    setNumberOfNodesForVersionPerDatacenter(VERSION_2, 0, 1);
                    return allVersions;
                })
                .thenAnswer(invocation -> {
                    cleanupVersions();
                    setNumberOfNodesForVersionPerDatacenter(VERSION_1, 3, 2);
                    setNumberOfNodesForVersionPerDatacenter(VERSION_UNREACHABLE, 1, 1);
                    setRfPerDatacenter(3, 3);
                    return allVersions;
                });

        when(config.schemaMutationTimeoutMillis()).thenReturn(10_000);
        assertWaitForSchemaVersionsDoesNotThrow();
        verify(client, times(3)).describe_schema_versions();
    }

    private void setNumberOfNodesForVersionPerDatacenter(String version, int dc1, int dc2) throws TException {
        versionToHostDc1.put(version, createHostList(DC1, version, dc1));
        versionToHostDc2.put(version, createHostList(DC2, version, dc2));

        allVersions = new HashMap<>(versionToHostDc1);
        versionToHostDc2.forEach((ver, hosts) -> allVersions
                .merge(ver, hosts, (hosts1, hosts2) -> Lists.newArrayList(Iterables.concat(hosts1, hosts2))));
    }

    private List<String> createHostList(String datacenter, String version, int number) {
        return IntStream.range(0, number)
                .mapToObj(num -> datacenter + version + num)
                .collect(Collectors.toList());
    }

    private void finalizeSchemaVersions() throws TException {
        when(client.describe_schema_versions()).thenReturn(allVersions);
    }

    private void setRfPerDatacenter(int rf1, int rf2) throws TException {
        Set<Set<String>> dc1Combs = Sets.combinations(getHosts(versionToHostDc1), rf1);
        Set<Set<String>> dc2Combs = Sets.combinations(getHosts(versionToHostDc2), rf2);
        Set<List<Set<String>>> allCombinations = Sets.cartesianProduct(ImmutableList.of(dc1Combs, dc2Combs));

        when(client.describe_ring(any())).thenReturn(allCombinations.stream()
                .map(hostList -> createTokenRange(hostList.get(0), hostList.get(1))).collect(Collectors.toList()));

        KsDef ksDef = new KsDef();
        ksDef.setStrategy_options(ImmutableMap.of(DC1, Integer.toString(rf1), DC2, Integer.toString(rf2)));
        when(client.describe_keyspace(any())).thenReturn(ksDef);
    }

    private Set<String> getHosts(Map<String, List<String>> versionToHost) {
        return versionToHost.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    private TokenRange createTokenRange(Collection<String> dc1Nodes, Collection<String> dc2Nodes) {
        List<EndpointDetails> endpointDetails = new ArrayList<>();
        dc1Nodes.forEach(host -> endpointDetails.add(new EndpointDetails(host, DC1)));
        dc2Nodes.forEach(host -> endpointDetails.add(new EndpointDetails(host, DC2)));
        return new TokenRange().setEndpoint_details(endpointDetails);
    }

    private void cleanupVersions() {
        versionToHostDc1 = new HashMap<>();
        versionToHostDc2 = new HashMap<>();
    }

    private void setOnlyTokenRangeToIncludeQuorumFromEachDc() throws TException {
        when(client.describe_ring(any())).thenReturn(ImmutableList.of(
                createTokenRange(
                        Lists.newArrayList(Iterables.concat(
                                versionToHostDc1.get(VERSION_1).subList(0, 2),
                                versionToHostDc1.get(VERSION_UNREACHABLE).subList(0, 1))),
                        Lists.newArrayList(Iterables.concat(
                                versionToHostDc2.get(VERSION_1).subList(0, 2),
                                versionToHostDc2.get(VERSION_UNREACHABLE).subList(0, 1))))));
    }

    private void assertWaitForSchemaVersionsDoesNotThrow() throws TException {
        CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE);
    }

    private void assertWaitForSchemaVersionsThrows() {
        assertThatThrownBy(() -> CassandraKeyValueServices.waitForSchemaVersions(config, client, TABLE))
                .isInstanceOf(IllegalStateException.class);
    }
}
