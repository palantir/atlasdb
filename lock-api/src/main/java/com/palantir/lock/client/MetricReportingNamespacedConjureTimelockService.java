/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class MetricReportingNamespacedConjureTimelockService implements NamespacedConjureTimelockService {
    private final NamespacedConjureTimelockService delegate;
    private final LeaderElectionMetrics metrics;
    private final Set<UUID> seenLeaderIds = Sets.newConcurrentHashSet();
    private volatile Optional<UUID> leaderId = Optional.empty();

    public MetricReportingNamespacedConjureTimelockService(
            NamespacedConjureTimelockService delegate,
            TaggedMetricRegistry taggedMetricRegistry) {
        this.delegate = delegate;
        this.metrics = LeaderElectionMetrics.of(taggedMetricRegistry);
    }

    @Override
    public ConjureUnlockResponse unlock(ConjureUnlockRequest request) {
        return delegate.unlock(request);
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(ConjureRefreshLocksRequest request) {
        return delegate.refreshLocks(request);
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(ConjureLockRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public ConjureLockResponse lock(ConjureLockRequest request) {
        return delegate.lock(request);
    }

    @Override
    public LeaderTime leaderTime() {
        return delegate.leaderTime();
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(GetCommitTimestampsRequest request) {
        return runTimed(() -> delegate.getCommitTimestamps(request), response -> response.getLockWatchUpdate().logId());
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        return delegate.getFreshTimestamps(request);
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return runTimed(() -> delegate.startTransactions(request), response -> response.getLockWatchUpdate().logId());
    }

    private <T> T runTimed(Supplier<T> method, Function<T, UUID> leaderExtractor) {
        Optional<UUID> maybeCurrentLeader = leaderId;
        if (!maybeCurrentLeader.isPresent()) {
            return method.get();
        }
        UUID currentLeader = maybeCurrentLeader.get();
        Stopwatch stopwatch = Stopwatch.createStarted();
        T response = method.get();
        Duration timeTaken = stopwatch.elapsed();
        if (!leaderExtractor.apply(response).equals(currentLeader)) {
            logMetrics(timeTaken);
        }
        return response;
    }

    private void logMetrics(Duration timeTaken) {
        metrics.observedDuration().update(timeTaken.toNanos(), TimeUnit.NANOSECONDS);
    }
}
