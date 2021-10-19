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

package com.palantir.lock.client;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.correctness.TimestampCorrectnessMetrics;
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
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import org.immutables.value.Value;

public final class TimestampCorroboratingTimelockService implements NamespacedConjureTimelockService {
    private static final SafeLogger log = SafeLoggerFactory.get(TimestampCorroboratingTimelockService.class);
    private static final String CLOCKS_WENT_BACKWARDS_MESSAGE = "It appears that clocks went backwards!";

    private final Runnable timestampViolationCallback;
    private final NamespacedConjureTimelockService delegate;
    private final AtomicLong lowerBoundFromTimestamps = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong lowerBoundFromTransactions = new AtomicLong(Long.MIN_VALUE);

    @VisibleForTesting
    TimestampCorroboratingTimelockService(
            Runnable timestampViolationCallback, NamespacedConjureTimelockService delegate) {
        this.timestampViolationCallback = timestampViolationCallback;
        this.delegate = delegate;
    }

    public static NamespacedConjureTimelockService create(
            String userNamespace,
            TaggedMetricRegistry taggedMetricRegistry,
            NamespacedConjureTimelockService delegate) {
        return new TimestampCorroboratingTimelockService(
                () -> TimestampCorrectnessMetrics.of(taggedMetricRegistry)
                        .timestampsGoingBackwards(userNamespace)
                        .inc(),
                delegate);
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
        return checkAndUpdateLowerBound(
                () -> delegate.getCommitTimestamps(request),
                GetCommitTimestampsResponse::getInclusiveLower,
                GetCommitTimestampsResponse::getInclusiveUpper,
                OperationType.TIMESTAMP);
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        return checkAndUpdateLowerBound(
                () -> delegate.getFreshTimestamps(request),
                ConjureGetFreshTimestampsResponse::getInclusiveLower,
                ConjureGetFreshTimestampsResponse::getInclusiveUpper,
                OperationType.TIMESTAMP);
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return checkAndUpdateLowerBound(
                () -> delegate.startTransactions(request),
                r -> r.getTimestamps().start(),
                r -> r.getTimestamps().start()
                        + ((r.getTimestamps().count() - 1L) * r.getTimestamps().interval()),
                OperationType.TRANSACTION);
    }

    private <T> T checkAndUpdateLowerBound(
            Supplier<T> timestampContainerSupplier,
            ToLongFunction<T> lowerBoundExtractor,
            ToLongFunction<T> upperBoundExtractor,
            OperationType operationType) {
        TimestampBounds timestampBounds = getTimestampBounds();
        T timestampContainer = timestampContainerSupplier.get();

        long lowerFreshTimestamp = lowerBoundExtractor.applyAsLong(timestampContainer);
        long upperFreshTimestamp = upperBoundExtractor.applyAsLong(timestampContainer);
        checkTimestamp(timestampBounds, operationType, lowerFreshTimestamp, upperFreshTimestamp);
        updateLowerBound(operationType, upperFreshTimestamp);
        return timestampContainer;
    }

    @VisibleForTesting
    TimestampBounds getTimestampBounds() {
        long threadLocalLowerBoundFromTimestamps = lowerBoundFromTimestamps.get();
        long threadLocalLowerBoundFromTransactions = lowerBoundFromTransactions.get();
        return ImmutableTimestampBounds.of(threadLocalLowerBoundFromTimestamps, threadLocalLowerBoundFromTransactions);
    }

    private void checkTimestamp(
            TimestampBounds bounds, OperationType type, long lowerFreshTimestamp, long upperFreshTimestamp) {
        if (lowerFreshTimestamp <= Math.max(bounds.boundFromTimestamps(), bounds.boundFromTransactions())) {
            timestampViolationCallback.run();
            throw clocksWentBackwards(bounds, type, lowerFreshTimestamp, upperFreshTimestamp);
        }
    }

    private static RuntimeException clocksWentBackwards(
            TimestampBounds bounds, OperationType type, long lowerFreshTimestamp, long upperFreshTimestamp) {
        RuntimeException runtimeException = new SafeRuntimeException(CLOCKS_WENT_BACKWARDS_MESSAGE);
        log.error(
                CLOCKS_WENT_BACKWARDS_MESSAGE + ": bounds were {}, operation {}, fresh timestamp of {}.",
                SafeArg.of("bounds", bounds),
                SafeArg.of("operationType", type),
                SafeArg.of("lowerFreshTimestamp", lowerFreshTimestamp),
                SafeArg.of("upperFreshTimestamp", upperFreshTimestamp),
                runtimeException);
        throw runtimeException;
    }

    private void updateLowerBound(OperationType type, long freshTimestamp) {
        if (type == OperationType.TIMESTAMP) {
            lowerBoundFromTimestamps.accumulateAndGet(freshTimestamp, Math::max);
        } else {
            lowerBoundFromTransactions.accumulateAndGet(freshTimestamp, Math::max);
        }
    }

    @Value.Immutable
    interface TimestampBounds {
        @Value.Parameter
        long boundFromTimestamps();

        @Value.Parameter
        long boundFromTransactions();
    }

    private enum OperationType {
        TIMESTAMP,
        TRANSACTION;
    }
}
