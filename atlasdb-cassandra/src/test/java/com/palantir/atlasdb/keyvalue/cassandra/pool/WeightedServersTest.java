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
package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import org.junit.Test;
import org.mockito.Mockito;

public class WeightedServersTest {

    @Test
    public void testWeightedHostsWithUniformActivity() {
        Map<CassandraServer, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                CassandraServer.from(new InetSocketAddress(0)), createMockClientPoolingContainerWithUtilization(10),
                CassandraServer.from(new InetSocketAddress(1)), createMockClientPoolingContainerWithUtilization(10),
                CassandraServer.from(new InetSocketAddress(2)), createMockClientPoolingContainerWithUtilization(10));

        NavigableMap<Integer, CassandraServer> result = WeightedServers.create(pools).hosts;

        int expectedWeight = result.firstEntry().getKey();
        int prevKey = 0;
        for (Map.Entry<Integer, CassandraServer> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            assertThat(currWeight).isEqualTo(expectedWeight);
            prevKey = entry.getKey();
        }
    }

    @Test
    public void testWeightedHostsWithLowActivityPool() {
        CassandraServer lowActivityHost = CassandraServer.from(new InetSocketAddress(2));
        Map<CassandraServer, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                CassandraServer.from(new InetSocketAddress(0)),
                createMockClientPoolingContainerWithUtilization(10),
                CassandraServer.from(new InetSocketAddress(1)),
                createMockClientPoolingContainerWithUtilization(10),
                lowActivityHost,
                createMockClientPoolingContainerWithUtilization(0));

        NavigableMap<Integer, CassandraServer> result = WeightedServers.create(pools).hosts;

        int largestWeight = result.firstEntry().getKey();
        CassandraServer hostWithLargestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, CassandraServer> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            prevKey = entry.getKey();
            if (currWeight > largestWeight) {
                largestWeight = currWeight;
                hostWithLargestWeight = entry.getValue();
            }
        }
        assertThat(hostWithLargestWeight).isEqualTo(lowActivityHost);
    }

    @Test
    public void testWeightedHostsWithMaxActivityPool() {
        CassandraServer highActivityHost = CassandraServer.from(new InetSocketAddress(2));
        Map<CassandraServer, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                CassandraServer.from(new InetSocketAddress(0)),
                createMockClientPoolingContainerWithUtilization(5),
                CassandraServer.from(new InetSocketAddress(1)),
                createMockClientPoolingContainerWithUtilization(5),
                highActivityHost,
                createMockClientPoolingContainerWithUtilization(20));

        NavigableMap<Integer, CassandraServer> result = WeightedServers.create(pools).hosts;

        int smallestWeight = result.firstEntry().getKey();
        CassandraServer hostWithSmallestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, CassandraServer> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            prevKey = entry.getKey();
            if (currWeight < smallestWeight) {
                smallestWeight = currWeight;
                hostWithSmallestWeight = entry.getValue();
            }
        }
        assertThat(hostWithSmallestWeight).isEqualTo(highActivityHost);
    }

    @Test
    public void testWeightedHostsWithNonZeroWeights() {
        Map<CassandraServer, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                CassandraServer.from(new InetSocketAddress(0)), createMockClientPoolingContainerWithUtilization(5),
                CassandraServer.from(new InetSocketAddress(1)), createMockClientPoolingContainerWithUtilization(10),
                CassandraServer.from(new InetSocketAddress(2)), createMockClientPoolingContainerWithUtilization(15));

        NavigableMap<Integer, CassandraServer> result = WeightedServers.create(pools).hosts;

        int prevKey = 0;
        for (Map.Entry<Integer, CassandraServer> entry : result.entrySet()) {
            assertThat(entry.getKey()).isGreaterThan(prevKey);
            prevKey = entry.getKey();
        }
    }

    // Covers a bug where we used ceilingEntry instead of higherEntry
    @Test
    public void testSelectingHostFromWeightedHostsMatchesWeight() {
        Map<CassandraServer, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                CassandraServer.from(new InetSocketAddress(0)), createMockClientPoolingContainerWithUtilization(5),
                CassandraServer.from(new InetSocketAddress(1)), createMockClientPoolingContainerWithUtilization(10),
                CassandraServer.from(new InetSocketAddress(2)), createMockClientPoolingContainerWithUtilization(15));
        WeightedServers weightedServers = WeightedServers.create(pools);
        Map<CassandraServer, Integer> hostsToWeight = new HashMap<>();
        int prevKey = 0;
        for (Map.Entry<Integer, CassandraServer> entry : weightedServers.hosts.entrySet()) {
            hostsToWeight.put(entry.getValue(), entry.getKey() - prevKey);
            prevKey = entry.getKey();
        }

        // Exhaustively test all indexes
        Map<CassandraServer, Integer> numTimesSelected = new HashMap<>();
        for (int index = 0; index < weightedServers.hosts.lastKey(); index++) {
            CassandraServer host = weightedServers.getRandomServerInternal(index);
            if (!numTimesSelected.containsKey(host)) {
                numTimesSelected.put(host, 0);
            }
            numTimesSelected.put(host, numTimesSelected.get(host) + 1);
        }

        assertThat(numTimesSelected).containsExactlyInAnyOrderEntriesOf(hostsToWeight);
    }

    private static CassandraClientPoolingContainer createMockClientPoolingContainerWithUtilization(int utilization) {
        CassandraClientPoolingContainer mock = Mockito.mock(CassandraClientPoolingContainer.class);
        Mockito.when(mock.getOpenRequests()).thenReturn(utilization);
        return mock;
    }
}
