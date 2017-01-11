/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.timestamp.impl;

import static java.util.concurrent.TimeUnit.MINUTES;

import static com.google.common.base.Preconditions.checkArgument;

public class AvailableTimestamps {
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final long MINIMUM_BUFFER = ALLOCATION_BUFFER_SIZE / 2;
    private static final long MAX_TIMESTAMPS_TO_HAND_OUT = 10 * 1000;

    private final LastReturnedTimestamp lastReturnedTimestamp;
    private final PersistentUpperLimit upperLimit;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        DebugLogger.logger.info("Creating AvailableTimestamps object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
    }

    public synchronized TimestampRange handOut(long numberToHandOut) {
        checkArgument(
                numberToHandOut <= MAX_TIMESTAMPS_TO_HAND_OUT,
                "Can only hand out %s timestamps at a time, but %s were requested",
                MAX_TIMESTAMPS_TO_HAND_OUT, numberToHandOut);

        long targetTimestamp = lastHandedOut() + numberToHandOut;
        DebugLogger.logger.trace("Handing out {} timestamps, taking us to {}.", numberToHandOut, targetTimestamp);
        return handOutTimestamp(targetTimestamp);
    }

    public synchronized void refreshBuffer() {
        long currentUpperLimit = upperLimit.get();
        long buffer = currentUpperLimit - lastHandedOut();

        if (buffer < MINIMUM_BUFFER || !upperLimit.hasIncreasedWithin(1, MINUTES)) {
            DebugLogger.logger.trace(
                    "refreshBuffer: refreshing and allocating timestamps. Buffer {}, Current upper limit {}.",
                    buffer,
                    currentUpperLimit);
            allocateEnoughTimestampsToHandOut(lastHandedOut() + ALLOCATION_BUFFER_SIZE);
        } else {
            DebugLogger.logger.trace("refreshBuffer: refreshing, but not allocating");
        }
    }

    public synchronized void fastForwardTo(long newMinimum) {
        lastReturnedTimestamp.increaseToAtLeast(newMinimum);
        upperLimit.increaseToAtLeast(newMinimum + ALLOCATION_BUFFER_SIZE);
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
        DebugLogger.logger.trace("Increasing limit to at least {}.", timestamp);
        upperLimit.increaseToAtLeast(timestamp);
        if (DebugLogger.logger.isTraceEnabled()) {
            DebugLogger.logger.trace("Increased to at least {}. Limit is now {}.", timestamp, getUpperLimit());
        }
    }

    public long getUpperLimit() {
        return upperLimit.get();
    }
}
