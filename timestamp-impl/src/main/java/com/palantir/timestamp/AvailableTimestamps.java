/**
 * Copyright 2016 Palantir Technologies
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

import static java.util.concurrent.TimeUnit.MINUTES;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;

public class AvailableTimestamps {
    protected static final long DEFAULT_ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final long MAX_TIMESTAMPS_TO_HAND_OUT = 10 * 1000;

    private final LastReturnedTimestamp lastReturnedTimestamp;
    private final PersistentUpperLimit upperLimit;
    private final long allocationBufferSize;
    private final long minimumBuffer;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        this(lastReturnedTimestamp, upperLimit, DEFAULT_ALLOCATION_BUFFER_SIZE);
    }

    @VisibleForTesting
    AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit, long allocationBufferSize) {
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
        this.allocationBufferSize = allocationBufferSize;
        minimumBuffer = allocationBufferSize / 2;
    }

    public synchronized TimestampRange handOut(long numberToHandOut) {
        checkArgument(
                numberToHandOut <= MAX_TIMESTAMPS_TO_HAND_OUT,
                "Can only hand out %s timestamps at a time, but %s were requested",
                MAX_TIMESTAMPS_TO_HAND_OUT, numberToHandOut);

        return handOutTimestamp(lastHandedOut() + numberToHandOut);
    }

    public synchronized void refreshBuffer() {
        long buffer = upperLimit.get() - lastHandedOut();

        if (buffer < minimumBuffer || !upperLimit.hasIncreasedWithin(1, MINUTES)) {
            System.out.println("*** Refreshing and allocating");
            allocateEnoughTimestampsToHandOut(lastHandedOut() + allocationBufferSize);
        } else {
            System.out.println("*** Refreshing, but not allocating");
        }
    }

    public synchronized void fastForwardTo(long newMinimum) {
        lastReturnedTimestamp.increaseToAtLeast(newMinimum);
        upperLimit.increaseToAtLeast(newMinimum + allocationBufferSize);
    }

    private long lastHandedOut() {
        return lastReturnedTimestamp.get();
    }

    private synchronized TimestampRange handOutTimestamp(long targetTimestamp) {
        checkArgument(
                targetTimestamp > lastHandedOut(),
                "Could not hand out timestamp '%s' as it was earlier than the last handed out timestamp: %s",
                targetTimestamp, lastHandedOut());

        TimestampRange rangeToHandOut = TimestampRange.createInclusiveRange(lastHandedOut() + 1, targetTimestamp);

        allocateEnoughTimestampsToHandOut(targetTimestamp);
        lastReturnedTimestamp.increaseToAtLeast(targetTimestamp);

        return rangeToHandOut;
    }

    private void allocateEnoughTimestampsToHandOut(long timestamp) {
        System.out.println("[TRACE:1] Increasing to " + timestamp);
        upperLimit.increaseToAtLeast(timestamp);
        System.out.println("[TRACE:4] Increasing done: " + timestamp);
    }

    public long getUpperLimit() {
        return upperLimit.get();
    }
}
