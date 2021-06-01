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

import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.logsafe.Preconditions;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Weights hosts inversely by the number of active connections. {@link #getRandomHost()} should then be used to
 * pick a random host
 */
public final class WeightedHosts {
    final NavigableMap<Integer, InetSocketAddress> hosts;

    private WeightedHosts(NavigableMap<Integer, InetSocketAddress> hosts) {
        this.hosts = hosts;
    }

    public static WeightedHosts create(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        Preconditions.checkArgument(!pools.isEmpty(), "pools should be non-empty");
        return new WeightedHosts(buildHostsWeightedByActiveConnections(pools));
    }

    /**
     * The key for a host is the open upper bound of the weight. Since the domain is intended to be contiguous, the
     * closed lower bound of that weight is the key of the previous entry.
     * <p>
     * The closed lower bound of the first entry is 0.
     * <p>
     * Every weight is guaranteed to be non-zero in size. That is, every key is guaranteed to be at least one larger
     * than the previous key.
     */
    private static NavigableMap<Integer, InetSocketAddress> buildHostsWeightedByActiveConnections(
            Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {

        Map<InetSocketAddress, Integer> openRequestsByHost = Maps.newHashMapWithExpectedSize(pools.size());
        int totalOpenRequests = 0;
        for (Map.Entry<InetSocketAddress, CassandraClientPoolingContainer> poolEntry : pools.entrySet()) {
            int openRequests = Math.max(poolEntry.getValue().getOpenRequests(), 0);
            openRequestsByHost.put(poolEntry.getKey(), openRequests);
            totalOpenRequests += openRequests;
        }

        int lowerBoundInclusive = 0;
        NavigableMap<Integer, InetSocketAddress> weightedHosts = new TreeMap<>();
        for (Map.Entry<InetSocketAddress, Integer> entry : openRequestsByHost.entrySet()) {
            // We want the weight to be inversely proportional to the number of open requests so that we pick
            // less-active hosts. We add 1 to make sure that all ranges are non-empty
            int weight = totalOpenRequests - entry.getValue() + 1;
            weightedHosts.put(lowerBoundInclusive + weight, entry.getKey());
            lowerBoundInclusive += weight;
        }
        return weightedHosts;
    }

    public InetSocketAddress getRandomHost() {
        int index = ThreadLocalRandom.current().nextInt(hosts.lastKey());
        return getRandomHostInternal(index);
    }

    // This basically exists for testing
    InetSocketAddress getRandomHostInternal(int index) {
        return hosts.higherEntry(index).getValue();
    }
}
