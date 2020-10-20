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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Snapshot;
import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.conjure.java.lib.SafeLong;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.feedback.LeaderElectionStatistics;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class LeaderElectionReportingTimelockService implements NamespacedConjureTimelockService {
    private static final Logger log = LoggerFactory.getLogger(LeaderElectionReportingTimelockService.class);
    private final NamespacedConjureTimelockService delegate;
    private final LeaderElectionMetrics metrics;
    private volatile UUID leaderId = null;

    public LeaderElectionReportingTimelockService(
            NamespacedConjureTimelockService delegate, TaggedMetricRegistry taggedMetricRegistry) {
        this.delegate = delegate;
        this.metrics = LeaderElectionMetrics.of(taggedMetricRegistry);
    }

    public static LeaderElectionReportingTimelockService create(
            ConjureTimelockService conjureTimelockService, String namespace) {
        return new LeaderElectionReportingTimelockService(
                new NamespacedConjureTimelockServiceImpl(conjureTimelockService, namespace),
                new DefaultTaggedMetricRegistry());
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
        return runTimed(() -> delegate.getCommitTimestamps(request), response -> response.getLockWatchUpdate()
                .logId());
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        return delegate.getFreshTimestamps(request);
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return runTimed(() -> delegate.startTransactions(request), response -> response.getLockWatchUpdate()
                .logId());
    }

    public LeaderElectionStatistics statistics() {
        Snapshot metricsSnapshot = metrics.observedDuration().getSnapshot();
        LeaderElectionStatistics meh = LeaderElectionStatistics.builder()
                .p99(metricsSnapshot.get99thPercentile())
                .p95(metricsSnapshot.get95thPercentile())
                .mean(metricsSnapshot.getMean())
                .count(SafeLong.of(metrics.observedDuration().getCount()))
                .build();
        resetTimer();
        return meh;
    }

    private <T> T runTimed(Supplier<T> method, Function<T, UUID> leaderExtractor) {
        UUID currentLeader = leaderId;
        Stopwatch stopwatch = Stopwatch.createStarted();
        T response = method.get();
        Duration timeTaken = stopwatch.elapsed();
        if (updateLeaderIfNew(leaderExtractor, currentLeader, response)) {
            logMetrics(timeTaken);
        }
        return response;
    }

    private <T> boolean updateLeaderIfNew(Function<T, UUID> leaderExtractor, UUID currentLeader, T response) {
        UUID newLeader = leaderExtractor.apply(response);
        boolean election = !newLeader.equals(currentLeader);

        if (election) {
            leaderId = newLeader;
        }

        return election && (currentLeader != null);
    }

    private void logMetrics(Duration timeTaken) {
        metrics.observedDuration().update(timeTaken.toNanos(), TimeUnit.NANOSECONDS);
        log.info("Leader election duration as observed by call", SafeArg.of("timeTaken", timeTaken));
    }
}
