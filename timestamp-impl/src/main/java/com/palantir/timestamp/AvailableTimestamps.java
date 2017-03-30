/*
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

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class AvailableTimestamps {
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final long MINIMUM_BUFFER = ALLOCATION_BUFFER_SIZE / 2;
    private static final long MAX_TIMESTAMPS_TO_HAND_OUT = 10 * 1000;

    @GuardedBy("this")
    private final LastReturnedTimestamp lastReturnedTimestamp;
    @GuardedBy("this")
    private final PersistentUpperLimit upperLimit;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        DebugLogger.logger.info("Creating AvailableTimestamps object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
    }

    private static void checkValidTimestampRangeRequest(long numberToHandOut) {
        if (numberToHandOut > MAX_TIMESTAMPS_TO_HAND_OUT) {
            // explicitly not using Preconditions to optimize hot success path and avoid allocations
            throw new IllegalArgumentException(String.format(
                    "Can only hand out %s timestamps at a time, but %s were requested",
                    MAX_TIMESTAMPS_TO_HAND_OUT, numberToHandOut));
        }
    }

    public TimestampRange handOut(long numberToHandOut) {
        checkValidTimestampRangeRequest(numberToHandOut);
        long targetTimestamp;
        TimestampRange timestampRange;

        synchronized (this) {
            /*
             * Under high concurrent load, this will be a hot critical section as clients request timestamps.
             * It is important to minimize contention as much as possible on this path.
             */
            targetTimestamp = lastHandedOut() + numberToHandOut;
            timestampRange = handOutTimestamp(targetTimestamp);
        }

        DebugLogger.logger.trace("Handing out {} timestamps, taking us to {}.", numberToHandOut, targetTimestamp);
        return timestampRange;
    }

    public void refreshBuffer() {
        boolean needsRefresh;
        long currentUpperLimit;
        long buffer;

        synchronized (this) {
            currentUpperLimit = upperLimit.get();
            long lastHandedOut = lastHandedOut();
            buffer = currentUpperLimit - lastHandedOut;
            needsRefresh = buffer < MINIMUM_BUFFER || !upperLimit.hasIncreasedWithin(1, TimeUnit.MINUTES);
            if (needsRefresh) {
                allocateEnoughTimestampsToHandOut(lastHandedOut + ALLOCATION_BUFFER_SIZE, 0L);
            }
        }

        if (DebugLogger.logger.isTraceEnabled()) {
            // explicitly avoiding boxing when logging disabled
            logRefresh(needsRefresh, currentUpperLimit, buffer);
        }
    }

    private static void logRefresh(boolean needsRefresh, long currentUpperLimit, long buffer) {
        if (needsRefresh) {
            DebugLogger.logger.trace("refreshBuffer: refreshing and allocating timestamps. "
                            + "Buffer {}, Current upper limit {}.",
                    buffer, currentUpperLimit);
        } else {
            DebugLogger.logger.trace("refreshBuffer: refreshing, but not allocating");
        }
    }

    public synchronized void fastForwardTo(long newMinimum) {
        lastReturnedTimestamp.increaseToAtLeast(newMinimum);
        upperLimit.increaseToAtLeast(newMinimum + ALLOCATION_BUFFER_SIZE, 0L);
    }

    private synchronized long lastHandedOut() {
        // synchronizing for semantic consistency and avoid `assert Thread.holdsLock(this);` overhead
        return lastReturnedTimestamp.get();
    }

    /**
     * Gets the range of timestamps to hand out.
     * @return timestamp range to hand out
     * @throws IllegalArgumentException if targetTimestamp is less than or equal to last handed out timestamp
     */
    private synchronized TimestampRange getRangeToHandOut(long targetTimestamp) {
        long lastHandedOut = lastHandedOut();
        if (targetTimestamp <= lastHandedOut) {
            // explicitly not using Preconditions to optimize hot success path and avoid allocations
            throw new IllegalArgumentException(String.format(
                    "Could not hand out timestamp '%s' as it was earlier than the last handed out timestamp: %s",
                    targetTimestamp, lastHandedOut));
        }
        return TimestampRange.createInclusiveRange(lastHandedOut + 1, targetTimestamp);
    }

    private synchronized TimestampRange handOutTimestamp(long targetTimestamp) {
        TimestampRange rangeToHandOut = getRangeToHandOut(targetTimestamp);
        allocateEnoughTimestampsToHandOut(targetTimestamp, ALLOCATION_BUFFER_SIZE);
        lastReturnedTimestamp.increaseToAtLeast(targetTimestamp);

        return rangeToHandOut;
    }

    /**
     * Ensure the upper limit is at least timestamp. If an update is required, increase it by an additional amount,
     * specified by buffer.
     * @param timestamp minimal upper limit for the timestamp bound.
     * @param buffer additional buffer to use in case an update is necessary.
     */
    private synchronized void allocateEnoughTimestampsToHandOut(long timestamp, long buffer) {
        // synchronizing for semantic consistency and avoid `assert Thread.holdsLock(this);` overhead
        DebugLogger.logger.trace("Increasing limit to at least {}.", timestamp);
        long newLimit = upperLimit.increaseToAtLeast(timestamp, buffer);
        if (DebugLogger.logger.isTraceEnabled()) {
            DebugLogger.logger.trace("Increased to at least {}. Limit is now {}.", timestamp, newLimit);
        }
    }

    public long getUpperLimit() {
        return upperLimit.get();
    }
}
