/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timestamp;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;

@ThreadSafe
public class PersistentTimestampService implements TimestampService, TimestampManagementService {
    private static final int MAX_REQUEST_RANGE_SIZE = 10 * 1000;

    private final AvailableTimestamps availableTimestamps;
    private final ExecutorService executor;
    private final AtomicBoolean isAllocationTaskSubmitted;
    private final MetricsManager metricsManager = new MetricsManager();

    public PersistentTimestampService(AvailableTimestamps availableTimestamps, ExecutorService executor) {
        DebugLogger.logger.info(
                "Creating PersistentTimestampService object on thread {}. This should only happen once.",
                Thread.currentThread().getName());

        this.availableTimestamps = availableTimestamps;
        this.executor = executor;
        this.isAllocationTaskSubmitted = new AtomicBoolean(false);
    }

    public static PersistentTimestampService create(TimestampBoundStore tbs) {
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(tbs);
        LastReturnedTimestamp lastReturned = new LastReturnedTimestamp(upperLimit.get());
        AvailableTimestamps availableTimestamps = new AvailableTimestamps(lastReturned, upperLimit);
        ExecutorService executor = PTExecutors.newSingleThreadExecutor(
                PTExecutors.newThreadFactory("Timestamp allocator", Thread.NORM_PRIORITY, true));

        return new PersistentTimestampService(availableTimestamps, executor);
    }

    @SuppressWarnings("unused") // used by product
    public long getUpperLimitTimestampToHandOutInclusive() {
        return availableTimestamps.getUpperLimit();
    }

    @Override
    public long getFreshTimestamp() {
        Timer.Context timer = metricsManager.registerTimer(PersistentTimestampService.class, "getSingleTimestamp").time();
        metricsManager.registerMeter(PersistentTimestampService.class, null, "countGetSingleTimestamp").mark();
        try {
            return getFreshTimestamps(1).getLowerBound();
        } catch (Exception e) {
            metricsManager.registerMeter(PersistentTimestampService.class, null, "countExceptionGetSingleTimestamp").mark();
            throw e;
        } finally {
            timer.stop();
        }
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        Timer.Context timer = metricsManager.registerTimer(PersistentTimestampService.class, "getMultipleTimestamps").time();
        metricsManager.registerMeter(PersistentTimestampService.class, null, "countGetMultipleTimestamps").mark();
        try {
        /*
         * Under high concurrent load, this will be a hot method as clients request timestamps.
         * It is important to minimize contention as much as possible on this path.
         */
            int numTimestampsToHandOut = cleanUpTimestampRequest(numTimestampsRequested);
            TimestampRange handedOut = availableTimestamps.handOut(numTimestampsToHandOut);
            asynchronouslyRefreshBuffer();
            return handedOut;
        } catch (Exception e) {
            metricsManager.registerMeter(PersistentTimestampService.class, null, "countExceptionGetMultipleTimestamps").mark();
            throw e;
        } finally {
            timer.stop();
        }
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        Preconditions.checkArgument(currentTimestamp != TimestampManagementService.SENTINEL_TIMESTAMP,
                "Cannot fast forward to the sentinel timestamp %s. If you accessed this timestamp service remotely"
                        + " this is likely due to specifying an incorrect query parameter.", currentTimestamp);
        availableTimestamps.fastForwardTo(currentTimestamp);
    }

    private static int cleanUpTimestampRequest(int numTimestampsRequested) {
        if (numTimestampsRequested <= 0) {
            // explicitly not using Preconditions to optimize hot success path and avoid allocations
            throw new IllegalArgumentException(String.format(
                    "Number of timestamps requested must be greater than zero, was %s", numTimestampsRequested));
        }

        return Math.min(numTimestampsRequested, MAX_REQUEST_RANGE_SIZE);
    }

    /**
     * Attempts to submit a task to refresh the buffer if one is not already in-flight; otherwise does nothing.
     */
    private void asynchronouslyRefreshBuffer() {
        if (isAllocationTaskSubmitted.compareAndSet(false, true)) {
            executor.submit(() -> {
                try {
                    availableTimestamps.refreshBuffer();
                } finally {
                    isAllocationTaskSubmitted.set(false);
                }
            });
        }
    }

}
