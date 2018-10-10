/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * A timestamp service decorator for introducing runtime validity checks on received timestamps.
 */
@ThreadSafe
public class CorroboratingTimestampService implements TimestampService {

    private final TimestampService delegate;
    private volatile long lowerBound = -1L;

    public CorroboratingTimestampService(TimestampService delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        long snapshot = lowerBound;
        long freshTimestamp = delegate.getFreshTimestamp();

        if (freshTimestamp <= snapshot) {
            throw clocksWentBackwards(snapshot, freshTimestamp);
        }
        updateLowerBound(freshTimestamp);
        return freshTimestamp;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        long snapshot = lowerBound;
        TimestampRange timestampRange = delegate.getFreshTimestamps(numTimestampsRequested);

        if (timestampRange.getLowerBound() <= snapshot) {
            throw clocksWentBackwards(snapshot, timestampRange.getLowerBound());
        }
        updateLowerBound(timestampRange.getUpperBound());
        return timestampRange;
    }

    private synchronized void updateLowerBound(long freshTimestamp) {
        lowerBound = Math.max(freshTimestamp, lowerBound);
    }

    private AssertionError clocksWentBackwards(long timestampLowerBound, long freshTimestamp) {
        String errorMessage = "Expected timestamp to be greater than %s, but a fresh timestamp was %s!";
        return new AssertionError(String.format(errorMessage, timestampLowerBound, freshTimestamp));
    }
}
