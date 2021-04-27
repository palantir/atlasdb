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
package com.palantir.atlasdb.transaction.impl;

import com.codahale.metrics.Meter;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public final class InstrumentedTimelockService<T> implements TimelockService<T> {
    private static final MetricName SUCCESSFUL_REQUEST_METRIC_NAME = MetricName.builder()
            .safeName(AtlasDbMetricNames.TIMELOCK_SUCCESSFUL_REQUEST)
            .build();
    private static final MetricName FAILED_REQUEST_METRIC_NAME = MetricName.builder()
            .safeName(AtlasDbMetricNames.TIMELOCK_FAILED_REQUEST)
            .build();

    private final TimelockService<T> timelockService;
    private final Meter success;
    private final Meter fail;

    private InstrumentedTimelockService(TimelockService<T> timelockService, MetricsManager metricsManager) {
        this.timelockService = timelockService;
        this.success = metricsManager.getTaggedRegistry().meter(SUCCESSFUL_REQUEST_METRIC_NAME);
        this.fail = metricsManager.getTaggedRegistry().meter(FAILED_REQUEST_METRIC_NAME);
    }

    public static <T> TimelockService<T> create(TimelockService<T> timelockService, MetricsManager metricsManager) {
        // The instrumentation here is used primarily for the health check, not for external viewing.
        metricsManager.doNotPublish(SUCCESSFUL_REQUEST_METRIC_NAME);
        metricsManager.doNotPublish(FAILED_REQUEST_METRIC_NAME);
        return new InstrumentedTimelockService<>(timelockService, metricsManager);
    }

    @Override
    public boolean isInitialized() {
        return timelockService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return executeWithRecord(timelockService::getFreshTimestamp);
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken, T transactionDigest) {
        return executeWithRecord(
                () -> timelockService.getCommitTimestamp(startTs, commitLocksToken, transactionDigest));
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return executeWithRecord(() -> timelockService.getFreshTimestamps(numTimestampsRequested));
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        return executeWithRecord(timelockService::lockImmutableTimestamp);
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return executeWithRecord(() -> timelockService.startIdentifiedAtlasDbTransactionBatch(count));
    }

    @Override
    public long getImmutableTimestamp() {
        return executeWithRecord(timelockService::getImmutableTimestamp);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return executeWithRecord(() -> timelockService.lock(request));
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return executeWithRecord(() -> timelockService.waitForLocks(request));
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return executeWithRecord(() -> timelockService.refreshLockLeases(tokens));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return executeWithRecord(() -> timelockService.unlock(tokens));
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        executeWithRecord(() -> {
            timelockService.tryUnlock(tokens);
            return null;
        });
    }

    @Override
    public long currentTimeMillis() {
        return executeWithRecord(timelockService::currentTimeMillis);
    }

    private <T> T executeWithRecord(Supplier<T> method) {
        try {
            T result = method.get();
            success.mark();
            return result;
        } catch (Exception e) {
            fail.mark();
            throw e;
        }
    }
}
