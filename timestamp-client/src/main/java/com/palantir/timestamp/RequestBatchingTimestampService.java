/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.common.base.Throwables;
import com.palantir.common.proxy.TimingProxy;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.timer.LoggingOperationTimer;

/**
 * This uses smart batching to queue up requests and send them all as one larger batch.
 * @author carrino
 */
@ThreadSafe
public class RequestBatchingTimestampService implements TimestampService {
    private static final OperationTimer timer = LoggingOperationTimer.create(RequestBatchingTimestampService.class);
    private static final Logger log = LoggerFactory.getLogger(RequestBatchingTimestampService.class);

    public static final long DEFAULT_MIN_TIME_BETWEEN_REQUESTS = 0L;

    @GuardedBy("this")
    private long lastRequestTimeNanos = 0;
    private final long minTimeBetweenRequestsMillis;

    private final TimestampService delegate;

    /* The currently outstanding remote call, if any. If it exists, it should be used,
     * thus batching remote calls. If there is none, one should be created and installed.
     * The creator of a call must populate it.
     */
    private final AtomicReference</* nullable */ TimestampHolder> currentBatch =
            new AtomicReference<TimestampHolder>();

    public RequestBatchingTimestampService(TimestampService delegate) {
        this(delegate, DEFAULT_MIN_TIME_BETWEEN_REQUESTS);
    }

    public RequestBatchingTimestampService(TimestampService delegate, long minTimeBetweenRequestsMillis) {
        this.delegate = TimingProxy.newProxyInstance(TimestampService.class, delegate, timer);
        this.minTimeBetweenRequestsMillis = minTimeBetweenRequestsMillis;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        Long result = null;
        do {
            JoinedBatch joinedBatch = joinBatch();

            TimestampHolder batch = joinedBatch.batch;
            if (joinedBatch.thisThreadOwnsBatch) {
                boolean populationSuccess = populateBatchAndInstallNewBatch(batch);
                if (!populationSuccess) {
                    continue;
                }
            }

            try {
                batch.awaitPopulation();
            } catch (InterruptedException e) {
                log.warn("Interrupted waiting for timestamp batch to populate. Trying another batch.", e);
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
            if (!batch.isFailed()) {
                result = batch.getValue();
            }
        } while (result == null);
        return result;
    }

    private static class JoinedBatch {
        public final TimestampHolder batch;
        public final boolean thisThreadOwnsBatch;

        JoinedBatch(TimestampHolder batch, boolean thisThreadOwnsBatch) {
            this.batch = batch;
            this.thisThreadOwnsBatch = thisThreadOwnsBatch;
        }
    }

    private static final int MAX_BATCH_JOIN_ATTEMPTS = 5;

    private JoinedBatch joinBatch() {
        @Nullable TimestampHolder batch = null;
        boolean installedFreshBatch = false;
        boolean joinedBatch = false;
        for (int joinAttempts = 0; !(installedFreshBatch || joinedBatch); joinAttempts++) {
            batch = currentBatch.get();
            if (batch != null) {
                joinedBatch = batch.joinBatch();
            }
            if (!joinedBatch) {
                // Try to install a new batch if there is none or the current one can't be used.
                TimestampHolder freshBatch = new TimestampHolder();
                if (joinAttempts <= MAX_BATCH_JOIN_ATTEMPTS) {
                    /* Replace only if the current batch is null. If we already read a null out, then we expect it's
                     * still there. If we read a non-null batch but couldn't join it, then it has been invalidated by
                     * its owner. But, before invalidating, the owner should have replaced it with null, so the value
                     * changed since we read it. That's why we should expect to see a null now even though we read a
                     * non-null earlier.
                     */
                    installedFreshBatch = currentBatch.compareAndSet(null, freshBatch);
                } else {
                    /* This thread got unlucky too many times. Specifically: it tried to join a batch, but couldn't, but
                     * there was already a new batch when it tried to publish its own; and this repeated
                     * MAX_BATCH_JOIN_ATTEMPTS times in a row. To guarantee progress, blindly install its current batch.
                     * This may overwrite a currently-outstanding batch if it gets unlucky yet again, but that will not
                     * break correctness. Instead, this batch will "steal" that one's participants, and the two will
                     * need to block on each other. All of this is inefficient but not incorrect, and doing it
                     * guarantees progress, and it should be rare to get this unlucky.
                     */
                    log.warn("Failed to join a timestamp batch {} times; blindly installing a batch. "
                            + "This should be rare!", MAX_BATCH_JOIN_ATTEMPTS);
                    currentBatch.set(freshBatch);
                    installedFreshBatch = true;
                }
                if (installedFreshBatch) {
                    batch = freshBatch;
                }
            }
        }
        Preconditions.checkState(batch != null, "Failed to join any timestamp batch");
        return new JoinedBatch(batch, installedFreshBatch);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    private synchronized boolean populateBatchAndInstallNewBatch(TimestampHolder batch) {
        sleepForRateLimiting();

        /* Only replace the current batch if it's the one this thread is populating.
         * Otherwise it has already been replaced, and it's some other thread's job to
         * replace the current batch.
         */
        currentBatch.compareAndSet(batch, null);

        try {
            int numTimestampsToGet = batch.getRequestCountAndSetInvalid();
            Preconditions.checkState(TimestampHolder.isRequestCountValid(numTimestampsToGet),
                    "Timestamp batch's number of requests has already been invalidated. "
                    + "This should not have happened yet. numTimestampsToGet = %s", numTimestampsToGet);

            // NOTE: At this point, we are sure no new requests for fresh timestamps
            // for "batch" can come in. We can now safely populate the batch
            // with fresh timestamps without violating any freshness guarantees.
            // TODO(jkong): probably need to adjust this formula
            TimestampRange freshTimestamps = delegate.getFreshTimestamps(numTimestampsToGet);

            batch.populate(freshTimestamps);
        } catch (Throwable t) {
            batch.fail();
            throw t;
        } finally {
            lastRequestTimeNanos = System.nanoTime();
            batch.becomeReadable();
        }

        return true;
    }

    private synchronized void sleepForRateLimiting() {
        if (minTimeBetweenRequestsMillis == 0) {
            return;
        }

        long nowNanos = System.nanoTime();
        long elapsedMillis = TimeUnit.MILLISECONDS.convert(
                nowNanos - lastRequestTimeNanos,
                TimeUnit.NANOSECONDS);
        long timeToSleepMillis = Math.max(0, minTimeBetweenRequestsMillis - elapsedMillis);

        if (timeToSleepMillis > 0) {
            try {
                Thread.sleep(timeToSleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static class TimestampHolder {
        private final AtomicInteger requestCount;
        private final CountDownLatch populationLatch;
        private volatile boolean failed;
        private volatile long endInclusive;
        private final AtomicLong valueToReturnNext;

        TimestampHolder() {
            requestCount = new AtomicInteger(1); // 1 because the current thread wants to make a request.

            // This will count down after it's been populated, which happens only once.
            populationLatch = new CountDownLatch(1);

            valueToReturnNext = new AtomicLong(-1L); // Dummy value. Will be populated.
            failed = false;
        }

        // The following methods must only be called by the thread that created this batch.
        public void populate(TimestampRange range) {
            this.endInclusive = range.getUpperBound();
            this.valueToReturnNext.set(range.getLowerBound());
        }
        public int getRequestCountAndSetInvalid() {
            return requestCount.getAndSet(Integer.MIN_VALUE);
        }
        public void fail() {
            failed = true;
        }
        public void becomeReadable() {
            populationLatch.countDown();
        }
        // End creator-only threads.

        public boolean isFailed() {
            return failed;
        }

        /**
         * Attempts to join a timestamp request batch.
         *
         * @return true if we are included in the batch and false otherwise
         */
        public boolean joinBatch() {
            if (requestCount.get() < 0) {
                return false;
            }
            int val = requestCount.incrementAndGet();
            return isRequestCountValid(val);
        }

        public void awaitPopulation() throws InterruptedException {
            populationLatch.await();
        }

        // This must only be called after awaitPopulation().
        public Long getValue() {
            long toReturn = valueToReturnNext.getAndIncrement();
            if (toReturn > endInclusive) {
                return null;
            }
            return toReturn;
        }


        public static boolean isRequestCountValid(int requestCount) {
            return requestCount > 0;
        }
    }
}
