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

import com.google.common.collect.ImmutableMap;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.immutables.value.Value;

public final class CassandraAbsentHostTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAbsentHostTracker.class);
    private final int requiredConsecutiveRequestsBeforeRemoval;
    private volatile Map<InetSocketAddress, PoolAndCount> absentHosts;

    public CassandraAbsentHostTracker(int requiredConsecutiveRequestsBeforeRemoval) {
        this.requiredConsecutiveRequestsBeforeRemoval = requiredConsecutiveRequestsBeforeRemoval;
        absentHosts = ImmutableMap.of();
    }

    @SuppressWarnings("NonAtomicVolatileUpdate")
    Optional<CassandraClientPoolingContainer> returnPoolIfPresent(InetSocketAddress host) {
        Optional<PoolAndCount> pool = Optional.ofNullable(absentHosts.get(host));
        absentHosts =
                KeyedStream.stream(absentHosts).filterKeys(h -> !h.equals(host)).collectToMap();
        return pool.map(PoolAndCount::container);
    }

    @SuppressWarnings("NonAtomicVolatileUpdate")
    void trackPoolToRemove(InetSocketAddress host, CassandraClientPoolingContainer pool) {
        absentHosts = ImmutableMap.<InetSocketAddress, PoolAndCount>builder()
                .putAll(absentHosts)
                .put(host, PoolAndCount.of(pool))
                .build();
    }

    @SuppressWarnings("NonAtomicVolatileUpdate")
    Set<InetSocketAddress> removeRepeatedlyAbsentServers() {
        Set<InetSocketAddress> removedServers = new HashSet<>();
        absentHosts = KeyedStream.stream(absentHosts)
                .map(PoolAndCount::incrementCount)
                .filterEntries((host, pool) -> {
                    if (pool.timesAbsent() >= requiredConsecutiveRequestsBeforeRemoval) {
                        try {
                            pool.container().shutdownPooling();
                            removedServers.add(host);
                        } catch (Exception e) {
                            log.warn(
                                    "While removing a host ({}) from the pool, we were unable to gently cleanup"
                                            + " resources.",
                                    SafeArg.of("removedServerAddress", CassandraLogHelper.host(host)),
                                    e);
                        }
                        return false;
                    } else {
                        return true;
                    }
                })
                .collectToMap();

        return removedServers;
    }

    @Value.Immutable
    interface PoolAndCount {
        CassandraClientPoolingContainer container();

        int timesAbsent();

        static PoolAndCount of(CassandraClientPoolingContainer pool) {
            return ImmutablePoolAndCount.builder()
                    .container(pool)
                    .timesAbsent(1)
                    .build();
        }

        default PoolAndCount incrementCount() {
            return ImmutablePoolAndCount.builder()
                    .from(this)
                    .timesAbsent(timesAbsent() + 1)
                    .build();
        }
    }
}
