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

import java.util.concurrent.TimeUnit;

import com.palantir.common.time.Clock;
import com.palantir.common.time.SystemClock;

public class PersistentUpperLimit {
    private final TimestampBoundStore tbs;
    private final Clock clock;
    private volatile long cachedValue;
    private volatile long lastIncreasedTime;

    public PersistentUpperLimit(TimestampBoundStore tbs, Clock clock) {
        this.tbs = tbs;
        this.clock = clock;

        cachedValue = tbs.getUpperLimit();
        lastIncreasedTime = clock.getTimeMillis();
    }

    public PersistentUpperLimit(TimestampBoundStore boundStore) {
        this(boundStore, new SystemClock());
    }

    private synchronized void store(long upperLimit) {
        tbs.storeUpperLimit(upperLimit);
        cachedValue = upperLimit;
        lastIncreasedTime = clock.getTimeMillis();
    }

    public long get() {
        return cachedValue;
    }

    public synchronized void increaseToAtLeast(long minimum) {
        if(cachedValue < minimum) {
            store(minimum);
        }
    }

    public boolean hasIncreasedWithin(int time, TimeUnit unit) {
        long durationInMillis = unit.toMillis(time);
        long timeSinceIncrease = clock.getTimeMillis() - lastIncreasedTime;

        return timeSinceIncrease < durationInMillis;
    }
}
