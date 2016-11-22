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

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory implementation of timestamp service. There are a few legitimate uses for this in
 * production code, but primarily this should only be used in test code.
 *
 * @author bdorne
 *
 */
public class InMemoryTimestampService implements TimestampService, TimestampAdminService {
    private final AtomicLong counter = new AtomicLong(0);
    private boolean valid = true;

    @Override
    public long getFreshTimestamp() {
        assertServiceValidity();
        return counter.incrementAndGet();
    }

    private void assertServiceValidity() {
        if (!valid) {
            throw new IllegalStateException("This timestamp service is invalid!");
        }
    }

    @Override
    public TimestampRange getFreshTimestamps(int timestampsToGet) {
        assertServiceValidity();
        if (timestampsToGet <= 0) {
            throw new IllegalArgumentException("Argument must be positive: " + timestampsToGet);
        }
        long topValue = counter.addAndGet(timestampsToGet);
        return TimestampRange.createInclusiveRange(topValue - timestampsToGet + 1, topValue);
    }

    @Override
    public void fastForwardTimestamp(long newMinimumTimestamp) {
        long currentValue = counter.get();
        while (currentValue < newMinimumTimestamp) {
            if (counter.compareAndSet(currentValue, newMinimumTimestamp)) {
                valid = true;
                return;
            }
            currentValue = counter.get();
        }
    }

    @Override
    public void invalidateTimestamps() {
        counter.set(Long.MIN_VALUE);
        valid = false;
    }
}
