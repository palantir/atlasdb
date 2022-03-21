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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Weights hosts inversely by the number of active connections. {@link #getRandomServer()} should then be used to
 * pick a random host
 */
public final class WeightedServers {
    final NavigableMap<Integer, CassandraServer> hosts;

    private WeightedServers(NavigableMap<Integer, CassandraServer> hosts) {
        this.hosts = hosts;
    }

    public static WeightedServers create(Map<CassandraServer, CassandraClientPoolingContainer> pools) {
        Preconditions.checkArgument(!pools.isEmpty(), "pools should be non-empty");
        return new WeightedServers(buildHostsWeightedByActiveConnections(pools));
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
    private static NavigableMap<Integer, CassandraServer> buildHostsWeightedByActiveConnections(
            Map<CassandraServer, CassandraClientPoolingContainer> pools) {

        Map<CassandraServer, Integer> openRequestsByHost = Maps.newHashMapWithExpectedSize(pools.size());
        int totalOpenRequests = 0;
        for (Map.Entry<CassandraServer, CassandraClientPoolingContainer> poolEntry : pools.entrySet()) {
            int openRequests = Math.max(poolEntry.getValue().getOpenRequests(), 0);
            openRequestsByHost.put(poolEntry.getKey(), openRequests);
            totalOpenRequests += openRequests;
        }

        int lowerBoundInclusive = 0;
        NavigableMap<Integer, CassandraServer> weightedHosts = new TreeMap<>();
        for (Map.Entry<CassandraServer, Integer> entry : openRequestsByHost.entrySet()) {
            // We want the weight to be inversely proportional to the number of open requests so that we pick
            // less-active hosts. We add 1 to make sure that all ranges are non-empty
            int weight = totalOpenRequests - entry.getValue() + 1;
            weightedHosts.put(lowerBoundInclusive + weight, entry.getKey());
            lowerBoundInclusive += weight;
        }
        return weightedHosts;
    }

    public CassandraServer getRandomServer() {
        int index = ThreadLocalRandom.current().nextInt(hosts.lastKey());
        return getRandomServerInternal(index);
    }

    // This basically exists for testing
    CassandraServer getRandomServerInternal(int index) {
        return hosts.higherEntry(index).getValue();
    }
}
