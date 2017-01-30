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

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting1.tracing.Tracers;

@ThreadSafe
public class PersistentTimestampService implements TimestampService, TimestampManagementService {
    private static final int MAX_REQUEST_RANGE_SIZE = 10 * 1000;

    private final ExecutorService executor;
    private final AvailableTimestamps availableTimestamps;

    public PersistentTimestampService(AvailableTimestamps availableTimestamps, ExecutorService executor) {
        DebugLogger.logger.info(
                "Creating PersistentTimestampService object on thread {}. This should only happen once.",
                Thread.currentThread().getName());

        this.availableTimestamps = availableTimestamps;
        this.executor = executor;
    }

    public static PersistentTimestampService create(TimestampBoundStore tbs) {
        PersistentUpperLimit upperLimit = new PersistentUpperLimit(tbs);
        LastReturnedTimestamp lastReturned = new LastReturnedTimestamp(upperLimit.get());
        AvailableTimestamps availableTimestamps = new AvailableTimestamps(lastReturned, upperLimit);
        ExecutorService executor = Tracers.wrap(
                PTExecutors.newSingleThreadExecutor(
                        PTExecutors.newThreadFactory("Timestamp allocator", Thread.NORM_PRIORITY, true)));

        return new PersistentTimestampService(availableTimestamps, executor);
    }

    public long getUpperLimitTimestampToHandOutInclusive() {
        return availableTimestamps.getUpperLimit();
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public synchronized TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        int numTimestampsToHandOut = cleanUpTimestampRequest(numTimestampsRequested);
        TimestampRange handedOut = availableTimestamps.handOut(numTimestampsToHandOut);
        asynchronouslyRefreshBuffer();
        return handedOut;
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        availableTimestamps.fastForwardTo(currentTimestamp);
    }

    private int cleanUpTimestampRequest(int numTimestampsRequested) {
        Preconditions.checkArgument(numTimestampsRequested > 0,
                "Number of timestamps requested must be greater than zero, was %s",
                numTimestampsRequested);

        return Math.min(numTimestampsRequested, MAX_REQUEST_RANGE_SIZE);
    }

    private void asynchronouslyRefreshBuffer() {
        executor.submit(availableTimestamps::refreshBuffer);
    }
}
