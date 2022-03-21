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
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraNodeIdentifier;
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

public final class CassandraAbsentHostTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAbsentHostTracker.class);

    private final int absenceLimit;
    private final Map<CassandraNodeIdentifier, PoolAndCount> absentCassNodes;

    public CassandraAbsentHostTracker(int absenceLimit) {
        this.absenceLimit = absenceLimit;
        this.absentCassNodes = new HashMap<>();
    }

    public synchronized Optional<CassandraClientPoolingContainer> returnPool(CassandraNodeIdentifier nodeIdentifier) {
        return Optional.ofNullable(absentCassNodes.remove(nodeIdentifier)).map(PoolAndCount::container);
    }

    public synchronized void trackAbsentCassNode(
            CassandraNodeIdentifier absentNode, CassandraClientPoolingContainer pool) {
        absentCassNodes.putIfAbsent(absentNode, PoolAndCount.of(pool));
    }

    public synchronized Set<CassandraNodeIdentifier> incrementAbsenceAndRemove() {
        return cleanupAbsentHosts(ImmutableSet.copyOf(absentCassNodes.keySet()));
    }

    public synchronized void shutDown() {
        KeyedStream.stream(absentCassNodes).map(PoolAndCount::container).forEach(this::shutdownClientPoolForHost);
        absentCassNodes.clear();
    }

    private Set<CassandraNodeIdentifier> cleanupAbsentHosts(Set<CassandraNodeIdentifier> absentNodesSnapshot) {
        absentNodesSnapshot.forEach(this::incrementAbsenceCountIfPresent);
        return absentNodesSnapshot.stream()
                .map(this::removeIfAbsenceThresholdReached)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
    }

    private void incrementAbsenceCountIfPresent(CassandraNodeIdentifier cassNode) {
        absentCassNodes.computeIfPresent(cassNode, (_host, poolAndCount) -> poolAndCount.incrementCount());
    }

    private Optional<CassandraNodeIdentifier> removeIfAbsenceThresholdReached(CassandraNodeIdentifier cassNode) {
        if (absentCassNodes.get(cassNode).timesAbsent() <= absenceLimit) {
            return Optional.empty();
        } else {
            PoolAndCount removedServer = absentCassNodes.remove(cassNode);
            shutdownClientPoolForHost(cassNode, removedServer.container());
            return Optional.of(cassNode);
        }
    }

    private void shutdownClientPoolForHost(
            CassandraNodeIdentifier cassNode, CassandraClientPoolingContainer container) {
        try {
            container.shutdownPooling();
        } catch (Exception e) {
            log.warn(
                    "While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    SafeArg.of("removedServerAddress", CassandraLogHelper.cassandraHost(cassNode)),
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
