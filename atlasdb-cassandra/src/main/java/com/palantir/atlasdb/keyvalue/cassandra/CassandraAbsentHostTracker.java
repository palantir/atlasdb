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
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;
import org.immutables.value.Value;

public final class CassandraAbsentHostTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAbsentHostTracker.class);

    private final int absenceLimit;

    @GuardedBy("this")
    private final Map<CassandraServer, PoolAndCount> absentCassandraServers;

    public CassandraAbsentHostTracker(int absenceLimit) {
        this.absenceLimit = absenceLimit;
        this.absentCassandraServers = new HashMap<>();
    }

    public synchronized Optional<CassandraClientPoolingContainer> returnPool(CassandraServer cassandraServer) {
        return Optional.ofNullable(absentCassandraServers.remove(cassandraServer))
                .map(PoolAndCount::container);
    }

    public synchronized void trackAbsentCassandraServer(
            CassandraServer absentServer, CassandraClientPoolingContainer pool) {
        absentCassandraServers.putIfAbsent(absentServer, PoolAndCount.of(pool));
    }

    public synchronized Set<CassandraServer> incrementAbsenceAndRemove() {
        return cleanupAbsentServer(ImmutableSet.copyOf(absentCassandraServers.keySet()));
    }

    public synchronized void shutDown() {
        KeyedStream.stream(absentCassandraServers).map(PoolAndCount::container).forEach(this::shutdownClientPool);
        absentCassandraServers.clear();
    }

    private ImmutableSet<CassandraServer> cleanupAbsentServer(ImmutableSet<CassandraServer> absentServersSnapshot) {
        absentServersSnapshot.forEach(this::incrementAbsenceCountIfPresent);
        return absentServersSnapshot.stream()
                .map(this::removeIfAbsenceThresholdReached)
                .flatMap(Optional::stream)
                .collect(ImmutableSet.toImmutableSet());
    }

    private synchronized void incrementAbsenceCountIfPresent(CassandraServer cassandraServer) {
        absentCassandraServers.computeIfPresent(
                cassandraServer, (_host, poolAndCount) -> poolAndCount.incrementCount());
    }

    private synchronized Optional<CassandraServer> removeIfAbsenceThresholdReached(CassandraServer cassandraServer) {
        if (absentCassandraServers.get(cassandraServer).timesAbsent() <= absenceLimit) {
            return Optional.empty();
        } else {
            PoolAndCount removedServer = absentCassandraServers.remove(cassandraServer);
            shutdownClientPool(cassandraServer, removedServer.container());
            return Optional.of(cassandraServer);
        }
    }

    private void shutdownClientPool(CassandraServer cassandraServer, CassandraClientPoolingContainer container) {
        try {
            container.shutdownPooling();
        } catch (Exception e) {
            log.warn(
                    "While removing a host ({}) from the pool, we were unable to gently cleanup resources.",
                    SafeArg.of("removedServer", cassandraServer),
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
