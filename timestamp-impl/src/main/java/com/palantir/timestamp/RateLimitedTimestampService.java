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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.common.proxy.TimingProxy;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.timer.LoggingOperationTimer;

/**
 * This uses smart batching to queue up requests and send them all as one larger batch.
 * @author carrino
 */
@ThreadSafe
public class RateLimitedTimestampService implements TimestampService {
    private final static OperationTimer timer = LoggingOperationTimer.create(RateLimitedTimestampService.class);
    private static final Logger log = LoggerFactory.getLogger(RateLimitedTimestampService.class);

    @GuardedBy("this")
    private long lastRequestTimeNanos = 0;
    private final long minTimeBetweenRequestsMillis;

    private final TimestampService delegate;

    private volatile TimestampHolder currentBatch = new TimestampHolder();

    public RateLimitedTimestampService(TimestampService delegate, long minTimeBetweenRequestsMillis) {
        this.delegate = TimingProxy.newProxyInstance(TimestampService.class, delegate, timer);
        this.minTimeBetweenRequestsMillis = minTimeBetweenRequestsMillis;
    }

    @Override
    public long getFreshTimestamp() {
        Long result = null;
        do {
            TimestampHolder batch = currentBatch;
            if (!batch.incrementRequestCount()) {
                // We didn't get included in this batch so we should just get in on the next one.
                continue;
            }
            synchronized (batch) {
                if (!batch.isPopulated()) {
                    boolean populated = populateBatchAndInstallNewBatch(batch);
                    if (!populated) {
                        // This batch went bad. Try a new one.
                        continue;
                    }
                }
                result = batch.getValue();
            }
        } while (result == null);
        return result;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return delegate.getFreshTimestamps(numTimestampsRequested);
    }

    private synchronized boolean populateBatchAndInstallNewBatch(TimestampHolder batch) {
        sleepForRateLimiting();

        currentBatch = new TimestampHolder();

        // NOTE: At this point, we are sure no new requests for fresh timestamps
        // for "batch" can come in. We can now safely populate the batch
        // with fresh timestamps without violating any freshness guarantees.

        // TODO: probably need to adjust this formula
        int numTimestampsToGet = batch.getRequestCountAndSetInvalid();
        if (!TimestampHolder.isRequestCountValid(numTimestampsToGet)) {
            log.warn("Skipping populating timestamps request. It looks like something went wrong with the " +
                    "previous attempt to populate. Request count: {}", numTimestampsToGet);
            return false;
        }
        batch.populate(delegate.getFreshTimestamps(numTimestampsToGet));

        lastRequestTimeNanos = System.nanoTime();
        return true;
    }

    private void sleepForRateLimiting() {
        long nowNanos = System.nanoTime();
        long elapsedMillis = TimeUnit.MILLISECONDS.convert(
                nowNanos - lastRequestTimeNanos,
                TimeUnit.NANOSECONDS);
        long timeToSleepMillis = minTimeBetweenRequestsMillis - elapsedMillis;

        timeToSleepMillis = Math.max(0, timeToSleepMillis);

        if (timeToSleepMillis > 0) {
            try {
                Thread.sleep(timeToSleepMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @ThreadSafe
    static class TimestampHolder {
        final AtomicInteger requestCount = new AtomicInteger(0);
        @GuardedBy("this") boolean isPopulated = false;
        @GuardedBy("this") long endInclusive;
        @GuardedBy("this") long valueToReturnNext;

        public void populate(TimestampRange range) {
            this.endInclusive = range.getUpperBound();
            this.valueToReturnNext = range.getLowerBound();
            isPopulated = true;
        }

        public Long getValue() {
            Preconditions.checkState(isPopulated);
            if (!hasNext()) {
                return null;
            }
            return valueToReturnNext++;
        }

        private boolean hasNext() {
            return valueToReturnNext <= endInclusive;
        }

        public synchronized boolean isPopulated() {
            return isPopulated;
        }

        /**
         * @return true if we are included in the batch and false otherwise
         */
        public boolean incrementRequestCount() {
            if (requestCount.get() < 0) {
                return false;
            }
            int val = requestCount.incrementAndGet();
            return isRequestCountValid(val);
        }

        public int getRequestCountAndSetInvalid() {
            return requestCount.getAndSet(Integer.MIN_VALUE);
        }

        public static boolean isRequestCountValid(int requestCount) {
            return requestCount > 0;
        }
    }
}
