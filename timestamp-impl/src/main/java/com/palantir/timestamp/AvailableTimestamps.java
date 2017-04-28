/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class AvailableTimestamps {
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final long MINIMUM_BUFFER = ALLOCATION_BUFFER_SIZE / 2;
    private static final long MAX_TIMESTAMPS_TO_HAND_OUT = 10 * 1000;
    private final Object issueTimestampLock;

    private final LastReturnedTimestamp lastReturnedTimestamp;
    private final PersistentUpperLimit upperLimit;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        DebugLogger.logger.info("Creating AvailableTimestamps object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
        this.issueTimestampLock = new Object();
    }

    private static void checkValidTimestampRangeRequest(long numberToHandOut) {
        if (numberToHandOut > MAX_TIMESTAMPS_TO_HAND_OUT) {
            // explicitly not using Preconditions to optimize hot success path and avoid allocations
            throw new IllegalArgumentException(String.format(
                    "Can only hand out %s timestamps at a time, but %s were requested",
                    MAX_TIMESTAMPS_TO_HAND_OUT, numberToHandOut));
        }
    }

    /**
     * Hand out the requested timestampRange, only increase the upper bound if strictly necessary. Do not block on the
     * refresh of buffer if the existing buffer is already large enough.
     *
     * Modifies lastReturnedTimestamp and possibly upperLimit (but obtains a lock on this if it does).
     *
     * Possible race condition with refreshBuffer:
     * targetTimestamp > upperLimit.get(), indicating that we need to increase the timestamp bound, but upperLimit
     * gets increased before we obtain the lock in allocateEnoughTimestampsToHandOut. This is, however, not a problem
     * because the check in upperLimit.increaseToAtLeast will use the fresh value of upperLimit.get() and therefore
     * exit the method without performing an unnecessary cas.
     *
     * @param numberToHandOut number of timestamps
     * @return timestamp range handed out
     */
    public TimestampRange handOut(long numberToHandOut) {
        checkValidTimestampRangeRequest(numberToHandOut);
        long targetTimestamp;
        TimestampRange timestampRange;

        synchronized (issueTimestampLock) {
            /*
             * Under high concurrent load, this will be a hot critical section as clients request timestamps.
             * It is important to minimize contention as much as possible on this path.
             */
            long lastHandedOut = lastReturnedTimestamp.get();
            targetTimestamp = lastHandedOut + numberToHandOut;
            timestampRange = TimestampRange.createInclusiveRange(lastHandedOut + 1, targetTimestamp);
            if (targetTimestamp > upperLimit.get()) {
                // this is the only situation where we actually must block and wait for CAS
                allocateEnoughTimestampsToHandOut(targetTimestamp, ALLOCATION_BUFFER_SIZE);
            }
            lastReturnedTimestamp.increaseToAtLeast(targetTimestamp);
        }

        DebugLogger.logger.trace("Handing out {} timestamps, taking us to {}.", numberToHandOut, targetTimestamp);
        return timestampRange;
    }

    /**
     * Refreshes the buffer by increasing the timestamp bound if the buffer has become too small or 1 minute has passed
     * since the last increase.
     *
     * Modifies upperLimit.
     *
     * Possible race condition with handOut:
     * lastReturnedTimestamp increases after lastHandedOut is fixed. As a consequence, we might think the buffer is
     * larger than it is, and we may update upperLimit to a slightly lower bound. This is a good tradeoff since it means
     * we avoided blocking handouts in the meantime and upperLimit.increaseToAtLeast makes sure the bound never
     * decreases.
     */
    public void refreshBuffer() {
        boolean needsRefresh;
        long currentUpperLimit;
        long bufferUpperBound;

        synchronized (this) {
            currentUpperLimit = upperLimit.get();
            long lastHandedOut = lastReturnedTimestamp.get();
            bufferUpperBound = currentUpperLimit - lastHandedOut;
            needsRefresh = bufferUpperBound < MINIMUM_BUFFER || !upperLimit.hasIncreasedWithin(1, TimeUnit.MINUTES);
            if (needsRefresh) {
                allocateEnoughTimestampsToHandOut(lastHandedOut + ALLOCATION_BUFFER_SIZE, 0L);
            }
        }

        if (DebugLogger.logger.isTraceEnabled()) {
            // explicitly avoiding boxing when logging disabled
            logRefresh(needsRefresh, currentUpperLimit, bufferUpperBound);
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

    /**
     * Fast forwards to newMinimum.
     *
     * The order of synchronization is important to avoid possible deadlock with handOut -- the lock on
     * issueTimestampLock must be acquired before the lock on this.
     *
     * @param newMinimum new minimum timestamp
     */
    public void fastForwardTo(long newMinimum) {
        synchronized (issueTimestampLock) {
            synchronized (this) {
                lastReturnedTimestamp.increaseToAtLeast(newMinimum);
                upperLimit.increaseToAtLeast(newMinimum + ALLOCATION_BUFFER_SIZE, 0L);
            }
        }
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
