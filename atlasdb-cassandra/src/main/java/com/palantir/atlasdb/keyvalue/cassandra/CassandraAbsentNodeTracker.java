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

import com.google.common.collect.ImmutableSet;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class CassandraAbsentNodeTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAbsentNodeTracker.class);

    private final int absenceLimit;
    private final Map<CassandraNodeIdentifier, PoolAndCount> absentHosts;

    public CassandraAbsentNodeTracker(int absenceLimit) {
        this.absenceLimit = absenceLimit;
        this.absentHosts = new HashMap<>();
    }

    public synchronized Optional<CassandraClientPoolingContainer> returnPool(CassandraNodeIdentifier nodeIdentifier) {
        return Optional.ofNullable(absentHosts.remove(nodeIdentifier)).map(PoolAndCount::container);
    }

    public synchronized void trackAbsentHost(CassandraNodeIdentifier node, CassandraClientPoolingContainer pool) {
        absentHosts.putIfAbsent(node, PoolAndCount.of(pool));
    }

    public synchronized Set<CassandraNodeIdentifier> incrementAbsenceAndRemove() {
        return cleanupAbsentHosts(ImmutableSet.copyOf(absentHosts.keySet()));
    }

    public synchronized void shutDown() {
        KeyedStream.stream(absentHosts).map(PoolAndCount::container).forEach(this::shutdownClientPoolForHost);
        absentHosts.clear();
    }

    private Set<CassandraNodeIdentifier> cleanupAbsentHosts(Set<CassandraNodeIdentifier> absentNodes) {
        absentNodes.forEach(this::incrementAbsenceCountIfPresent);
        return absentNodes.stream()
                .map(this::removeIfAbsenceThresholdReached)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
    }

    private void incrementAbsenceCountIfPresent(CassandraNodeIdentifier nodeIdentifier) {
        absentHosts.computeIfPresent(nodeIdentifier, (_host, poolAndCount) -> poolAndCount.incrementCount());
    }

    private Optional<CassandraNodeIdentifier> removeIfAbsenceThresholdReached(CassandraNodeIdentifier nodeIdentifier) {
        if (absentHosts.get(nodeIdentifier).timesAbsent() <= absenceLimit) {
            return Optional.empty();
        } else {
            PoolAndCount removedServer = absentHosts.remove(nodeIdentifier);
            shutdownClientPoolForHost(nodeIdentifier, removedServer.container());
            return Optional.of(nodeIdentifier);
        }
    }

    private void shutdownClientPoolForHost(
            CassandraNodeIdentifier inetSocketAddress, CassandraClientPoolingContainer container) {
        try {
            container.shutdownPooling();
        } catch (Exception e) {
            log.warn(
                    "While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    SafeArg.of("removedServerAddress", CassandraLogHelper.host(inetSocketAddress)),
                    e);
        }
    }

    @Value.Immutable
    interface PoolAndCount {
        CassandraClientPoolingContainer container();

        int timesAbsent();

        static PoolAndCount of(CassandraClientPoolingContainer pool) {
            return ImmutablePoolAndCount.builder()
                    .container(pool)
                    .timesAbsent(0)
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
