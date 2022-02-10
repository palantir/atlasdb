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

public class WeightedHostsTest {

    @Test
    public void testWeightedHostsWithUniformActivity() {
        Map<DcAwareHost, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                getHost(0), createMockClientPoolingContainerWithUtilization(10),
                getHost(1), createMockClientPoolingContainerWithUtilization(10),
                getHost(2), createMockClientPoolingContainerWithUtilization(10));

        NavigableMap<Integer, DcAwareHost> result = WeightedHosts.create(pools).hosts;

        int expectedWeight = result.firstEntry().getKey();
        int prevKey = 0;
        for (Map.Entry<Integer, DcAwareHost> entry : result.entrySet()) {
            int currWeight = entry.getKey() - prevKey;
            assertThat(currWeight).isEqualTo(expectedWeight);
            prevKey = entry.getKey();
        }
    }

    @Test
    public void testWeightedHostsWithLowActivityPool() {
        DcAwareHost lowActivityHost = getHost(2);
        Map<DcAwareHost, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                getHost(0),
                createMockClientPoolingContainerWithUtilization(10),
                getHost(1),
                createMockClientPoolingContainerWithUtilization(10),
                lowActivityHost,
                createMockClientPoolingContainerWithUtilization(0));

        NavigableMap<Integer, DcAwareHost> result = WeightedHosts.create(pools).hosts;

        int largestWeight = result.firstEntry().getKey();
        DcAwareHost hostWithLargestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, DcAwareHost> entry : result.entrySet()) {
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
        DcAwareHost highActivityHost = getHost(2);
        Map<DcAwareHost, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                getHost(0),
                createMockClientPoolingContainerWithUtilization(5),
                getHost(1),
                createMockClientPoolingContainerWithUtilization(5),
                highActivityHost,
                createMockClientPoolingContainerWithUtilization(20));

        NavigableMap<Integer, DcAwareHost> result = WeightedHosts.create(pools).hosts;

        int smallestWeight = result.firstEntry().getKey();
        DcAwareHost hostWithSmallestWeight = result.firstEntry().getValue();
        int prevKey = 0;
        for (Map.Entry<Integer, DcAwareHost> entry : result.entrySet()) {
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
        Map<DcAwareHost, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                getHost(0), createMockClientPoolingContainerWithUtilization(5),
                getHost(1), createMockClientPoolingContainerWithUtilization(10),
                getHost(2), createMockClientPoolingContainerWithUtilization(15));

        NavigableMap<Integer, DcAwareHost> result = WeightedHosts.create(pools).hosts;

        int prevKey = 0;
        for (Map.Entry<Integer, DcAwareHost> entry : result.entrySet()) {
            assertThat(entry.getKey()).isGreaterThan(prevKey);
            prevKey = entry.getKey();
        }
    }

    // Covers a bug where we used ceilingEntry instead of higherEntry
    @Test
    public void testSelectingHostFromWeightedHostsMatchesWeight() {
        Map<DcAwareHost, CassandraClientPoolingContainer> pools = ImmutableMap.of(
                getHost(0), createMockClientPoolingContainerWithUtilization(5),
                getHost(1), createMockClientPoolingContainerWithUtilization(10),
                getHost(2), createMockClientPoolingContainerWithUtilization(15));
        WeightedHosts weightedHosts = WeightedHosts.create(pools);
        Map<DcAwareHost, Integer> hostsToWeight = new HashMap<>();
        int prevKey = 0;
        for (Map.Entry<Integer, DcAwareHost> entry : weightedHosts.hosts.entrySet()) {
            hostsToWeight.put(entry.getValue(), entry.getKey() - prevKey);
            prevKey = entry.getKey();
        }

        // Exhaustively test all indexes
        Map<DcAwareHost, Integer> numTimesSelected = new HashMap<>();
        for (int index = 0; index < weightedHosts.hosts.lastKey(); index++) {
            DcAwareHost host = weightedHosts.getRandomHostInternal(index);
            if (!numTimesSelected.containsKey(host)) {
                numTimesSelected.put(host, 0);
            }
            numTimesSelected.put(host, numTimesSelected.get(host) + 1);
        }

        assertThat(numTimesSelected).isEqualTo(hostsToWeight);
    }

    private DcAwareHost getHost(int port) {
        return DcAwareHost.of("foo", new InetSocketAddress(port));
    }

    private static CassandraClientPoolingContainer createMockClientPoolingContainerWithUtilization(int utilization) {
        CassandraClientPoolingContainer mock = Mockito.mock(CassandraClientPoolingContainer.class);
        Mockito.when(mock.getOpenRequests()).thenReturn(utilization);
        return mock;
    }
}
