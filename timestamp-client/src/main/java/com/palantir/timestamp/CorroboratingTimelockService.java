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

public class CorroboratingTimelockService implements TimestampService {

    private final TimestampService delegate;
    private volatile long lowerBound = -1L;

    public CorroboratingTimelockService(TimestampService timestampService) {
        this.delegate = timestampService;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        long freshTimestamp = delegate.getFreshTimestamp();
        updateLowerBound(freshTimestamp);
        return freshTimestamp;
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        TimestampRange timestampRange = delegate.getFreshTimestamps(numTimestampsRequested);
        updateLowerBound(timestampRange);
        return timestampRange;
    }

    private synchronized void updateLowerBound(long freshTimestamp) {
        if (freshTimestamp <= lowerBound) {
            throw new IllegalStateException("timestamps went back in time");
        }
        lowerBound = freshTimestamp;
    }

    private synchronized void updateLowerBound(TimestampRange timestampRange) {
        if (timestampRange.getLowerBound() <= lowerBound) {
            throw new IllegalStateException("timestamps went back in time");
        }
        lowerBound = timestampRange.getUpperBound();
    }
}
