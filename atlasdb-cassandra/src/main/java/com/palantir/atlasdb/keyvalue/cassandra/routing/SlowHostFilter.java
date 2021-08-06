/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.routing;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPoolingContainer;
import com.palantir.logsafe.SafeArg;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import one.util.streamex.EntryStream;
import one.util.streamex.StreamEx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowHostFilter implements HostFilter {

    private static final Logger log = LoggerFactory.getLogger(SlowHostFilter.class);

    private final double slownessThreshold;
    private final Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools;

    public SlowHostFilter(
            double slownessThreshold, Map<InetSocketAddress, CassandraClientPoolingContainer> currentPools) {
        this.slownessThreshold = slownessThreshold;
        this.currentPools = currentPools;
    }

    private static double getAverageWithoutSelf(double sum, double self, int count) {
        return (sum - self) / (count - 1);
    }

    @Override
    public Set<InetSocketAddress> filter(Set<InetSocketAddress> desiredHosts) {

        if (slownessThreshold == Double.MAX_VALUE) {
            return desiredHosts;
        }

        Map<InetSocketAddress, Snapshot> latencies = StreamEx.of(desiredHosts)
                .mapToEntry(currentPools::get)
                .mapValues(CassandraClientPoolingContainer::getLatency)
                .mapValues(ExponentiallyDecayingReservoir::getSnapshot)
                .toMap();

        double totalP99s = EntryStream.of(latencies)
                .values()
                .mapToDouble(Snapshot::get99thPercentile)
                .map(value -> Math.max(1, value)) // Otherwise the average will always be 1 if all but one host is 0
                .sum();

        return EntryStream.of(latencies)
                .mapValues(Snapshot::get99thPercentile)
                .mapValues(p99 -> p99 / getAverageWithoutSelf(totalP99s, p99, desiredHosts.size()))
                .removeKeyValue((host, p99) -> {
                    if (p99 >= slownessThreshold) {
                        log.warn(
                                "Removing slow host due to p99s being slower than other hosts",
                                SafeArg.of("p99", p99),
                                SafeArg.of("slowHostThreshold", slownessThreshold),
                                SafeArg.of("host", host));
                        return true;
                    }
                    return false;
                })
                .keys()
                .toSet();
    }
}
