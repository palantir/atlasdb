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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;
import com.palantir.exception.PalantirInterruptedException;

public class PersistentUpperLimit {
    private static final Logger log = LoggerFactory.getLogger(PersistentUpperLimit.class);

    private final TimestampBoundStore tbs;
    private final Clock clock;
    private final TimestampAllocationFailures allocationFailures;

    @GuardedBy("this")
    private volatile long cachedValue;
    private volatile long lastIncreasedTime;

    public PersistentUpperLimit(TimestampBoundStore tbs, Clock clock, TimestampAllocationFailures allocationFailures) {
        log.trace("Creating PersistentUpperLimit object. This should only happen once.");
        this.tbs = tbs;
        this.clock = clock;
        this.allocationFailures = checkNotNull(allocationFailures);

        cachedValue = tbs.getUpperLimit();
        lastIncreasedTime = clock.getTimeMillis();
    }

    public PersistentUpperLimit(TimestampBoundStore boundStore) {
        this(boundStore, new SystemClock(), new TimestampAllocationFailures());
    }

    public long get() {
        return cachedValue;
    }

    public synchronized void increaseToAtLeast(long minimum) {
        if (cachedValue < minimum) {
            store(minimum);
        } else {
            log.trace("Not storing upper limit of {}, as the cached value {} was higher.", minimum, cachedValue);
        }
    }

    public boolean hasIncreasedWithin(int time, TimeUnit unit) {
        long durationInMillis = unit.toMillis(time);
        long timeSinceIncrease = clock.getTimeMillis() - lastIncreasedTime;

        return timeSinceIncrease < durationInMillis;
    }

    private synchronized void store(long upperLimit) {
        log.trace("Storing new upper limit of {}.", upperLimit);
        checkWeHaveNotBeenInterrupted();
        allocationFailures.verifyWeShouldTryToAllocateMoreTimestamps();
        persistNewUpperLimit(upperLimit);
        cachedValue = upperLimit;
        lastIncreasedTime = clock.getTimeMillis();
        log.trace("Stored; upper limit is now {}.", upperLimit);
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
