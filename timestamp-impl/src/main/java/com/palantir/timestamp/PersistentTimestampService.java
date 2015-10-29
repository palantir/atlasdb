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
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.common.time.Clock;
import com.palantir.exception.PalantirInterruptedException;

@ThreadSafe
public class PersistentTimestampService implements TimestampService {
    private static final Logger log = LoggerFactory.getLogger(PersistentTimestampService.class);

    private static final int MAX_REQUEST_RANGE_SIZE = 10 * 1000;
    static final long ALLOCATION_BUFFER_SIZE = 1000 * 1000;
    private static final int ONE_MINUTE_IN_MILLIS = 60000;

    private final TimestampBoundStore store;

    private final AtomicLong lastReturnedTimestamp;
    private final AtomicLong upperLimitToHandOutInclusive;

    private final ExecutorService executor;
    private final AtomicBoolean isAllocationTaskSubmitted;

    private Clock clock;
    private long lastAllocatedTime;

    public static PersistentTimestampService create(TimestampBoundStore tbs) {
        PersistentTimestampService ts = new PersistentTimestampService(
                tbs,
                tbs.getUpperLimit(),
                new Clock() {
                    @Override
                    public long getTimeMillis() {
                        return System.currentTimeMillis();
                    }
                });
        return init(ts);
    }

    @VisibleForTesting
    protected static PersistentTimestampService create(TimestampBoundStore tbs, Clock clock) {
        PersistentTimestampService ts = new PersistentTimestampService(tbs, tbs.getUpperLimit(), clock);
        return init(ts);
    }

    private static PersistentTimestampService init(PersistentTimestampService ts) {
        ts.allocateMoreTimestamps();
        return ts;
    }

    private PersistentTimestampService(TimestampBoundStore tbs, long lastUpperBound, Clock clock) {
        store = tbs;
        lastReturnedTimestamp = new AtomicLong(lastUpperBound);
        upperLimitToHandOutInclusive = new AtomicLong(lastUpperBound);
        executor = PTExecutors.newSingleThreadExecutor(PTExecutors.newThreadFactory("Timestamp allocator", Thread.NORM_PRIORITY, true));
        isAllocationTaskSubmitted = new AtomicBoolean(false);
        this.clock = clock;
        lastAllocatedTime = clock.getTimeMillis();
    }

    public long getUpperLimitTimestampToHandOutInclusive() {
        return upperLimitToHandOutInclusive.get();
    }

    private synchronized void allocateMoreTimestamps() {
        long newLimit = lastReturnedTimestamp.get() + ALLOCATION_BUFFER_SIZE;
        store.storeUpperLimit(newLimit);
        // Prevent upper limit from falling behind stored upper limit.
        advanceAtomicLongToValue(upperLimitToHandOutInclusive, newLimit);
    }

    private static void advanceAtomicLongToValue(AtomicLong toAdvance, long val) {
        while (true) {
            long oldUpper = toAdvance.get();
            if (val <= oldUpper || toAdvance.compareAndSet(oldUpper, val)) {
                return;
            }
        }
    }

    volatile Throwable allocationFailure = null;
    private void submitAllocationTask() {
        if (isAllocationTaskSubmitted.compareAndSet(false, true)) {
            final Exception createdException = new Exception("allocation task called from here");
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (allocationFailure instanceof MultipleRunningTimestampServiceError) {
                            // We cannot allocate timestamps anymore because another server is running.
                            return;
                        }
                        allocateMoreTimestamps();
                        lastAllocatedTime = clock.getTimeMillis();
                        allocationFailure = null;
                    } catch (Throwable e) { // (authorized)
                        createdException.initCause(e);
                        if (allocationFailure != null
                                && e.getClass().equals(allocationFailure.getClass())) {
                            // QA-75825: don't keep logging error if we keep failing to allocate.
                            log.info("Throwable while allocating timestamps.", createdException);
                        } else {
                            log.error("Throwable while allocating timestamps.", createdException);
                        }
                        allocationFailure = e;
                    } finally {
                        isAllocationTaskSubmitted.set(false);
                    }
                }
            });
        }
    }

    private static boolean isAllocationRequired(long lastVal, long upperLimit) {
        return (upperLimit - lastVal) <= ALLOCATION_BUFFER_SIZE / 2;
    }

    @Override
    public long getFreshTimestamp() {
        long ret = getFreshTimestamps(1).getLowerBound();
        return ret;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        Preconditions.checkArgument(numTimestampsRequested > 0,
                "Number of timestamps requested must be greater than zero, was %s",
                numTimestampsRequested);

        if (numTimestampsRequested > MAX_REQUEST_RANGE_SIZE) {
            numTimestampsRequested = MAX_REQUEST_RANGE_SIZE;
        }
        boolean hasLogged = false;
        while (true) {
            long upperLimit = upperLimitToHandOutInclusive.get();
            long lastVal = lastReturnedTimestamp.get();
            if (lastVal >= upperLimit) {
                submitAllocationTask();
                Throwable possibleFailure = allocationFailure;
                if (possibleFailure instanceof MultipleRunningTimestampServiceError) {
                    throw new ServiceNotAvailableException("This server is no longer valid because another is running.", possibleFailure);
                } else if (possibleFailure != null) {
                    throw new RuntimeException("failed to allocate more timestamps", possibleFailure);
                }
                if (!hasLogged) {
                    log.error("We haven't gotten enough timestamps from the DB", new RuntimeException());
                    hasLogged = true;
                }
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    throw new PalantirInterruptedException("Interrupted while waiting for timestamp allocation.");
                }
                continue;
            }
            long newVal = Math.min(upperLimit, lastVal + numTimestampsRequested);
            if (lastReturnedTimestamp.compareAndSet(lastVal, newVal)) {
                // we allocate new timestamps if we have less than half of our allocation buffer left
                // or we haven't allocated timestamps in the last sixty seconds. The latter case
                // exists in order to log errors faster against the class of bugs where your
                // timestamp limit changed unexpectedly (usually, multiple TS against the same DB)
                if (isAllocationRequired(newVal, upperLimit) ||
                        lastAllocatedTime + ONE_MINUTE_IN_MILLIS < clock.getTimeMillis()) {
                    submitAllocationTask();
                }
                return TimestampRange.createInclusiveRange(lastVal + 1, newVal);
            }
        }
    }
}
