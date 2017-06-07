/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

public class PersistentTimestamp {

    private final PersistentUpperLimit upperLimit;
    private final AtomicTimestamp timestamp;

    public PersistentTimestamp(PersistentUpperLimit upperLimit, long lastKnownValue) {
        this.upperLimit = upperLimit;
        this.timestamp = new AtomicTimestamp(lastKnownValue);

        DebugLogger.createdPersistentTimestamp();
    }

    public TimestampRange incrementBy(long delta) {
        TimestampRange range = timestamp.incrementBy(delta);
        upperLimit.increaseToAtLeast(range.getUpperBound());

        return range;
    }

    public void increaseTo(long newTimestamp) {
        timestamp.increaseTo(newTimestamp);
        upperLimit.increaseToAtLeast(newTimestamp);
    }

    public long getUpperLimitTimestampToHandOutInclusive() {
        return upperLimit.get();
    }
}
