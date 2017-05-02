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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.exception.PalantirInterruptedException;

public class PersistentUpperLimit {
    private final TimestampBoundStore tbs;
    private final Clock clock;
    private final TimestampAllocationFailures allocationFailures;

    private volatile long cachedValue;
    private volatile long lastIncreasedTime;

    public PersistentUpperLimit(TimestampBoundStore tbs, Clock clock, TimestampAllocationFailures allocationFailures) {
        DebugLogger.logger.info("Creating PersistentUpperLimit object on thread {}. This should only happen once.",
                Thread.currentThread().getName());
        this.tbs = tbs;
        this.clock = clock;
        this.allocationFailures = checkNotNull(allocationFailures);

        cachedValue = tbs.getUpperLimit();
        lastIncreasedTime = clock.getTimeMillis();
    }

    public PersistentUpperLimit(TimestampBoundStore boundStore) {
        this(boundStore, new SystemClock(), new TimestampAllocationFailures());
    }

    /**
     * Gets the current upper limit timestamp.
     * @return upper limit timestamp
     */
    public long get() {
        allocationFailures.verifyWeShouldIssueMoreTimestamps();
        return cachedValue;
    }

    /**
     * Increases the stored upper limit to at least the minimum value. The store is only performed if the cached value
     * is lower than the minimum. If the check is successful, the value stored will be equal to
     * minimum + additionalBuffer. This way we can avoid persisting a new limit if not necessary, but also add a buffer
     * when the CAS is necessary.
     * @param minimum minimum upper limit to be persisted. The CAS will only be invoked if the current cached value
     * is lower than minimum.
     * @param additionalBuffer if a CAS is necessary, this value determines the additional buffer on top of minimum to
     * be persisted.
     * @return currently cached value
     */
    public synchronized long increaseToAtLeast(long minimum, long additionalBuffer) {
        long currentValue = get();
        if (currentValue < minimum) {
            return store(minimum + additionalBuffer);
        } else {
            DebugLogger.logger.trace(
                    "Not storing upper limit of {}, as the cached value {} was higher.",
                    minimum,
                    currentValue);
            return currentValue;
        }
    }

    /**
     * Determines if the upper limit has changed within the most recent specified period.
     * @param time time
     * @param unit time unit
     * @return true if the upper limit has increased within the most recent specified period
     */
    public boolean hasIncreasedWithin(int time, TimeUnit unit) {
        long durationInMillis = unit.toMillis(time);
        long timeSinceIncrease = clock.getTimeMillis() - lastIncreasedTime;

        return timeSinceIncrease < durationInMillis;
    }

    /**
     * Stores the upper limit.
     * @param upperLimit new upper limit
     * @return the stored upper limit
     */
    private synchronized long store(long upperLimit) {
        DebugLogger.logger.trace("Storing new upper limit of {}.", upperLimit);
        checkWeHaveNotBeenInterrupted();
        allocationFailures.verifyWeShouldIssueMoreTimestamps();
        persistNewUpperLimit(upperLimit);
        cachedValue = upperLimit;
        lastIncreasedTime = clock.getTimeMillis();
        DebugLogger.logger.trace("Stored; upper limit is now {}.", upperLimit);
        return upperLimit;
    }

    private void checkWeHaveNotBeenInterrupted() {
        if (Thread.currentThread().isInterrupted()) {
            throw new PalantirInterruptedException("Was interrupted while trying to allocate more timestamps");
        }
    }

    private void persistNewUpperLimit(long upperLimit) {
        try {
            tbs.storeUpperLimit(upperLimit);
        } catch (Throwable error) {
            throw allocationFailures.responseTo(error);
        }
    }

}
