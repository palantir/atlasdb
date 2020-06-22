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

import com.palantir.lock.v2.AutoDelegate_TimelockService;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.timestamp.TimestampRange;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * A timelock service decorator for introducing runtime validity checks on received timestamps.
 */
public final class TimestampCorroboratingTimelockService implements AutoDelegate_TimelockService {
    private static final String CLOCKS_WENT_BACKWARDS_MESSAGE =
            "Expected timestamp to be greater than %s, but a fresh timestamp was %s!";

    private final TimelockService delegate;
    private final LongAccumulator lowerBound = new LongAccumulator(Long::max, Long.MIN_VALUE);

    private TimestampCorroboratingTimelockService(TimelockService delegate) {
        this.delegate = delegate;
    }

    public static TimelockService create(TimelockService delegate) {
        return new TimestampCorroboratingTimelockService(delegate);
    }

    @Override
    public TimelockService delegate() {
        return delegate;
    }

    @Override
    public long getFreshTimestamp() {
        return checkAndUpdateLowerBound(delegate::getFreshTimestamp, x -> x, x -> x);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return checkAndUpdateLowerBound(() -> delegate.getFreshTimestamps(numTimestampsRequested),
                TimestampRange::getLowerBound,
                TimestampRange::getUpperBound);
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        return checkAndUpdateLowerBound(() -> delegate.startIdentifiedAtlasDbTransactionBatch(count),
                responses -> Collections.min(getTimestampsFromResponses(responses)),
                responses -> Collections.max(getTimestampsFromResponses(responses)));
    }

    private List<Long> getTimestampsFromResponses(List<StartIdentifiedAtlasDbTransactionResponse> responses) {
        return responses.stream().map(response -> response.startTimestampAndPartition().timestamp()).collect(
                Collectors.toList());
    }

    private <T> T checkAndUpdateLowerBound(Supplier<T> timestampContainerSupplier,
            ToLongFunction<T> lowerBoundExtractor,
            ToLongFunction<T> upperBoundExtractor) {
        long threadLocalLowerBound = lowerBound.get();
        T timestampContainer = timestampContainerSupplier.get();

        checkTimestamp(threadLocalLowerBound, lowerBoundExtractor.applyAsLong(timestampContainer));
        updateLowerBound(upperBoundExtractor.applyAsLong(timestampContainer));
        return timestampContainer;
    }

    private static void checkTimestamp(long timestampLowerBound, long freshTimestamp) {
        if (freshTimestamp <= timestampLowerBound) {
            throw clocksWentBackwards(timestampLowerBound, freshTimestamp);
        }
    }

    private void updateLowerBound(long freshTimestamp) {
        lowerBound.accumulate(freshTimestamp);
    }

    private static AssertionError clocksWentBackwards(long timestampLowerBound, long freshTimestamp) {
        return new AssertionError(String.format(CLOCKS_WENT_BACKWARDS_MESSAGE, timestampLowerBound, freshTimestamp));
    }
}
