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
package com.palantir.atlasdb.timelock.atomix;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampServiceWithManagement;

import io.atomix.variables.DistributedLong;

public class AtomixTimestampService implements TimestampServiceWithManagement {
    /**
     * Maximum number of timestamps that may be granted at once.
     */
    @VisibleForTesting
    static final int MAX_GRANT_SIZE = 10_000;

    private final DistributedLong timestamp;

    public AtomixTimestampService(DistributedLong timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public long getFreshTimestamp() {
        return getFreshTimestamps(1).getLowerBound();
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        Preconditions.checkArgument(numTimestampsRequested > 0,
                "Must request at least one timestamp, requested: %s", numTimestampsRequested);
        Preconditions.checkArgument(numTimestampsRequested <= MAX_GRANT_SIZE,
                "Must request at most %s timestamps, requested: %s", MAX_GRANT_SIZE, numTimestampsRequested);

        long lastTimestampHandedOut = AtomixRetryer.getWithRetry(() -> timestamp.getAndAdd(numTimestampsRequested));

        return TimestampRange.createInclusiveRange(
                lastTimestampHandedOut + 1,
                lastTimestampHandedOut + numTimestampsRequested);
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        long currentTimestampFromService = Futures.getUnchecked(timestamp.get());
        while (currentTimestampFromService < currentTimestamp) {
            if (Futures.getUnchecked(timestamp.compareAndSet(currentTimestampFromService, currentTimestamp))) {
                return;
            }
            currentTimestampFromService = Futures.getUnchecked(timestamp.get());
        }
    }
}
