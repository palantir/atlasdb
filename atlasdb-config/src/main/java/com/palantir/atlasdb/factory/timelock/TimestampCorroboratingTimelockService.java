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

package com.palantir.atlasdb.factory.timelock;

import com.palantir.atlasdb.keyvalue.api.cache.TransactionDigest;
import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.timestamp.TimestampRange;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A timelock service decorator for introducing runtime validity checks on received timestamps.
 */
public final class TimestampCorroboratingTimelockService implements AutoDelegate_TimelockService<TransactionDigest> {
    private static final Logger log = LoggerFactory.getLogger(TimestampCorroboratingTimelockService.class);
    private static final String CLOCKS_WENT_BACKWARDS_MESSAGE = "It appears that clocks went backwards!";

    private final TimelockService<TransactionDigest> delegate;
    private final AtomicLong lowerBoundFromTimestamps = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong lowerBoundFromTransactions = new AtomicLong(Long.MIN_VALUE);

    private TimestampCorroboratingTimelockService(TimelockService<TransactionDigest> delegate) {
        this.delegate = delegate;
    }

    public static TimelockService<TransactionDigest> create(TimelockService<TransactionDigest> delegate) {
        return new TimestampCorroboratingTimelockService(delegate);
    }

    @Override
    public TimelockService<TransactionDigest> delegate() {
        return delegate;
    }

    @Override
    public long getFreshTimestamp() {
        return checkAndUpdateLowerBound(delegate::getFreshTimestamp, x -> x, x -> x, OperationType.TIMESTAMP);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return checkAndUpdateLowerBound(
                () -> delegate.getFreshTimestamps(numTimestampsRequested),
                TimestampRange::getLowerBound,
                TimestampRange::getUpperBound,
                OperationType.TIMESTAMP);
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return checkAndUpdateLowerBound(
                () -> delegate.startIdentifiedAtlasDbTransactionBatch(count),
                responses -> Collections.min(getTimestampsFromResponses(responses)),
                responses -> Collections.max(getTimestampsFromResponses(responses)),
                OperationType.TRANSACTION);
    }

    private List<Long> getTimestampsFromResponses(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
        return responses.stream()
                .map(response -> response.startTimestampAndPartition().timestamp())
                .collect(Collectors.toList());
    }

    private <T> T checkAndUpdateLowerBound(
            Supplier<T> timestampContainerSupplier,
            ToLongFunction<T> lowerBoundExtractor,
            ToLongFunction<T> upperBoundExtractor,
            OperationType operationType) {
        TimestampBounds timestampBounds = getTimestampBounds();
        T timestampContainer = timestampContainerSupplier.get();

        checkTimestamp(timestampBounds, operationType, lowerBoundExtractor.applyAsLong(timestampContainer));
        updateLowerBound(operationType, upperBoundExtractor.applyAsLong(timestampContainer));
        return timestampContainer;
    }

    private TimestampBounds getTimestampBounds() {
        long threadLocalLowerBoundFromTimestamps = lowerBoundFromTimestamps.get();
        long threadLocalLowerBoundFromTransactions = lowerBoundFromTransactions.get();
        return ImmutableTimestampBounds.of(threadLocalLowerBoundFromTimestamps, threadLocalLowerBoundFromTransactions);
    }

    private static void checkTimestamp(TimestampBounds bounds, OperationType type, long freshTimestamp) {
        if (freshTimestamp <= Math.max(bounds.boundFromTimestamps(), bounds.boundFromTransactions())) {
            throw clocksWentBackwards(bounds, type, freshTimestamp);
        }
    }

    private static RuntimeException clocksWentBackwards(
            TimestampBounds bounds, OperationType type, long freshTimestamp) {
        RuntimeException runtimeException = new SafeRuntimeException(CLOCKS_WENT_BACKWARDS_MESSAGE);
        log.error(
                CLOCKS_WENT_BACKWARDS_MESSAGE + ": bounds were {}, operation {}, fresh timestamp of {}.",
                SafeArg.of("bounds", bounds),
                SafeArg.of("operationType", type),
                SafeArg.of("freshTimestamp", freshTimestamp),
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
