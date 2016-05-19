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

public class PersistentUpperLimit {
    private final TimestampBoundStore tbs;
    private volatile long cachedValue;

    public PersistentUpperLimit(TimestampBoundStore tbs) {
        this.tbs = tbs;
        cachedValue = tbs.getUpperLimit();
    }

    public synchronized void store(long upperLimit) {
        tbs.storeUpperLimit(upperLimit);
        cachedValue = upperLimit;
    }

    public long get() {
        return cachedValue;
    }

    public synchronized void increaseToAtLeast(long minimum) {
        if(cachedValue < minimum) {
            store(minimum);
        }
    }
}
