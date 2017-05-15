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

import java.util.concurrent.atomic.AtomicLong;

public class AtomicTimestamp {

    private final AtomicLong value;

    public AtomicTimestamp(long initialValue) {
        value = new AtomicLong(initialValue);
    }

    public TimestampRange incrementBy(long delta) {
        long upperBound = value.addAndGet(delta);
        long lowerBound = upperBound - delta + 1L;
        return TimestampRange.createInclusiveRange(lowerBound, upperBound);
    }

    public void increaseTo(long target) {
        value.updateAndGet(current -> Math.max(current, target));
    }

}
