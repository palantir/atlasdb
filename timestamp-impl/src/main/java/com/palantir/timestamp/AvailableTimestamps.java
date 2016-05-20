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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.exception.PalantirInterruptedException;

public class AvailableTimestamps {
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final long MINIMUM_BUFFER = ALLOCATION_BUFFER_SIZE / 2;
    private static final long MAX_RANGE_SIZE = 10 * 1000;

    private static final Logger log = LoggerFactory.getLogger(AvailableTimestamps.class);

    private final LastReturnedTimestamp lastReturnedTimestamp;
    private final PersistentUpperLimit upperLimit;
    private volatile Throwable previousAllocationFailure;

    public AvailableTimestamps(LastReturnedTimestamp lastReturnedTimestamp, PersistentUpperLimit upperLimit) {
        this.lastReturnedTimestamp = lastReturnedTimestamp;
        this.upperLimit = upperLimit;
    }

    public synchronized TimestampRange handOut(long timestamp) {
        TimestampRange desiredRange = TimestampRange.createInclusiveRange(lastReturnedTimestamp.get() + 1, timestamp);

        checkArgument(
                timestamp > lastHandedOut(),
                "Could not hand out timestamp '%s' as it was earlier than the last handed out timestamp: %s",
                timestamp, lastHandedOut());

        checkArgument(
                desiredRange.size() < MAX_RANGE_SIZE,
                "Can only hand out %s timestamps at a time. Fulfilling the request for %s would require handing out %s timestamps",
                MAX_RANGE_SIZE, timestamp, desiredRange.size());

        allocateEnoughTimestampsFor(timestamp);
        lastReturnedTimestamp.increaseToAtLeast(timestamp);

        return desiredRange;
    }

    public synchronized long lastHandedOut() {
        return lastReturnedTimestamp.get();
    }

    public synchronized void refreshBuffer() {
        long buffer = upperLimit.get() - lastReturnedTimestamp.get();

        if(buffer < MINIMUM_BUFFER || !upperLimit.hasIncreasedWithin(1, MINUTES)) {
            allocateEnoughTimestampsFor(lastHandedOut() + ALLOCATION_BUFFER_SIZE);
        }
    }

    public synchronized void fastForwardTo(long newMinimum) {
        lastReturnedTimestamp.increaseToAtLeast(newMinimum);
        upperLimit.increaseToAtLeast(newMinimum + ALLOCATION_BUFFER_SIZE);
    }

    private void allocateEnoughTimestampsFor(long timestamp) {
        try {
            upperLimit.increaseToAtLeast(timestamp);
        } catch(Throwable e) {
            handleAllocationFailure(e);
        }
    }

    private synchronized void handleAllocationFailure(Throwable failure) {
        if (failure instanceof MultipleRunningTimestampServiceError) {
            throw new ServiceNotAvailableException("This server is no longer valid because another is running.", failure);
        }

        if (failure != null) {
            throw new RuntimeException("failed to allocate more timestamps", failure);
        }

        if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            throw new PalantirInterruptedException("Interrupted while waiting for timestamp allocation.");
        }

        if (previousAllocationFailure != null
                && failure.getClass().equals(previousAllocationFailure.getClass())) {
            // QA-75825: don't keep logging error if we keep failing to allocate.
            log.info("Throwable while allocating timestamps.", failure);
        } else {
            log.error("Throwable while allocating timestamps.", failure);
        }

        previousAllocationFailure = failure;
    }
}
