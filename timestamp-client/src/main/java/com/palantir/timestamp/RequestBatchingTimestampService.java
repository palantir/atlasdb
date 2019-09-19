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

package com.palantir.timestamp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.common.base.Throwables;
import com.palantir.common.proxy.TimingProxy;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.timer.LoggingOperationTimer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This uses smart batching to queue up requests and send them all as one larger batch.
 */
@ThreadSafe
public final class RequestBatchingTimestampService implements CloseableTimestampService {
    private static final OperationTimer timer = LoggingOperationTimer.create(RequestBatchingTimestampService.class);

    private final TimestampService delegate;
    private final DisruptorAutobatcher<Integer, TimestampRange> batcher;

    private RequestBatchingTimestampService(TimestampService delegate,
            DisruptorAutobatcher<Integer, TimestampRange> batcher) {
        this.delegate = delegate;
        this.batcher = batcher;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public void close() {
        batcher.close();
    }

    @Override
    public long getFreshTimestamp() {
        TimestampRange range = getFreshTimestamps(1);
        return range.getLowerBound();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        Preconditions.checkArgument(numTimestampsRequested > 0, "Must not request zero or negative timestamps");
        ListenableFuture<TimestampRange> range = batcher.apply(numTimestampsRequested);
        try {
            return range.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // bit goofy, but would match previous stack trace behaviour... Think there's some stuff at higher levels
            // which only looks one cause down, where it should really examine the whole cause chain.
            Throwables.throwIfUncheckedException(e.getCause());
            throw new RuntimeException(e);
        }
    }

    public static RequestBatchingTimestampService create(TimestampService untimedDelegate) {
        TimestampService delegate = TimingProxy.newProxyInstance(TimestampService.class, untimedDelegate, timer);
        DisruptorAutobatcher<Integer, TimestampRange> autobatcher = Autobatchers.independent(consumer(delegate))
                .safeLoggablePurpose("request-batching-timestamp-service")
                .build();
        return new RequestBatchingTimestampService(delegate, autobatcher);
    }

    @VisibleForTesting
    static Consumer<List<BatchElement<Integer, TimestampRange>>> consumer(TimestampService delegate) {
        return batch -> {
            long totalTimestamps = batch.stream().mapToLong(BatchElement::argument).reduce(0, Math::addExact);
            long startInclusive = 0;
            long endExclusive = 0;
            for (BatchElement<Integer, TimestampRange> element : batch) {
                int timestampsRequired = element.argument();
                if (element.argument() <= endExclusive - startInclusive) {
                    element.result().set(createExclusiveRange(startInclusive, startInclusive + timestampsRequired));
                    startInclusive += timestampsRequired;
                    totalTimestamps -= timestampsRequired;
                } else {
                    TimestampRange requested = getFreshTimestampsFromDelegate(
                            delegate, Ints.saturatedCast(totalTimestamps));
                    startInclusive = requested.getLowerBound();
                    endExclusive = Math.addExact(requested.getUpperBound(), 1);
                    int toTake = Math.min(Ints.checkedCast(endExclusive - startInclusive), timestampsRequired);
                    element.result().set(createExclusiveRange(startInclusive, startInclusive + toTake));
                    startInclusive += toTake;
                    totalTimestamps -= timestampsRequired;
                }
            }
        };
    }

    private static TimestampRange getFreshTimestampsFromDelegate(TimestampService timestampService, int timestamps) {
        if (timestamps == 1) {
            long timestamp = timestampService.getFreshTimestamp();
            return TimestampRange.createInclusiveRange(timestamp, timestamp);
        }
        return timestampService.getFreshTimestamps(timestamps);
    }

    private static TimestampRange createExclusiveRange(long start, long end) {
        Preconditions.checkArgument(end > start,
                "End is not ahead of start so cannot create an exclusive range");
        return TimestampRange.createInclusiveRange(start, end - 1);
    }
}
