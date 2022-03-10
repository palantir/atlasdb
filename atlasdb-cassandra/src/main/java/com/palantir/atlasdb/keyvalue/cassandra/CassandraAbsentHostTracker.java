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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.immutables.value.Value;

public final class CassandraAbsentHostTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(CassandraAbsentHostTracker.class);

    private final int requiredConsecutiveRequestsBeforeRemoval;
    private final ConcurrentMap<InetSocketAddress, PoolAndCount> absentHosts;

    public CassandraAbsentHostTracker(int requiredConsecutiveRequestsBeforeRemoval) {
        this.requiredConsecutiveRequestsBeforeRemoval = requiredConsecutiveRequestsBeforeRemoval;
        this.absentHosts = new ConcurrentHashMap<>();
    }

    public Optional<CassandraClientPoolingContainer> returnPool(InetSocketAddress host) {
        return Optional.ofNullable(absentHosts.remove(host)).map(PoolAndCount::container);
    }

    public void trackAbsentHost(InetSocketAddress host, CassandraClientPoolingContainer pool) {
        absentHosts.putIfAbsent(host, PoolAndCount.of(pool));
    }

    public Set<InetSocketAddress> incrementAbsenceRoundAndRemoveRepeatedlyAbsentHosts() {
        return cleanupAbsentHosts(ImmutableSet.copyOf(absentHosts.keySet()));
    }

    private Set<InetSocketAddress> cleanupAbsentHosts(Set<InetSocketAddress> absentHostsSnapshot) {
        absentHostsSnapshot.forEach(this::incrementAbsenceCountIfPresent);
        return absentHostsSnapshot.stream()
                .map(this::removeIfAbsenceThresholdReached)
                .flatMap(Optional::stream)
                .collect(Collectors.toSet());
    }

    private void incrementAbsenceCountIfPresent(InetSocketAddress host) {
        absentHosts.computeIfPresent(host, (_host, poolAndCount) -> poolAndCount.incrementCount());
    }

    private Optional<InetSocketAddress> removeIfAbsenceThresholdReached(InetSocketAddress inetSocketAddress) {
        Optional<PoolAndCount> maybePoolAndCount = Optional.ofNullable(absentHosts.get(inetSocketAddress));
        return maybePoolAndCount.map(poolAndCount -> {
            if (poolAndCount.timesAbsent() > requiredConsecutiveRequestsBeforeRemoval) {
                shutdownClientPool(
                        inetSocketAddress, absentHosts.remove(inetSocketAddress).container());
                return inetSocketAddress;
            }
            return null;
        });
    }

    private void shutdownClientPool(InetSocketAddress inetSocketAddress, CassandraClientPoolingContainer container) {
        try {
            container.shutdownPooling();
        } catch (Exception e) {
            log.warn(
                    "While removing a host ({}) from the pool, we were unable to gently cleanup" + " resources.",
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
