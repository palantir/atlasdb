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

package com.palantir.atlasdb.util;

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;

/**
 * A Gauge that takes a timestamp on each call to {@link #setValue(Long, long)} and {@link #setValue(Long, long)}, and
 * exposes a method {@link #getLatestTimestamp()} to retrieve the greatest timestamp observed so far.
 */
public class TimestampedAccumulatingValueMetric implements Gauge<Long> {
    private final AccumulatingValueMetric delegate;
    private final AtomicLong latestTimestamp = new AtomicLong(0);

    public TimestampedAccumulatingValueMetric(AccumulatingValueMetric delegate) {
        this.delegate = delegate;
    }

    public static TimestampedAccumulatingValueMetric create() {
        return new TimestampedAccumulatingValueMetric(new AccumulatingValueMetric());
    }

    @Override
    public Long getValue() {
        return delegate.getValue();
    }

    public void setValue(Long newValue, long timestamp) {
        forwardTimestampTo(timestamp);
        delegate.setValue(newValue);
    }

    public void accumulateValue(Long newValue, long timestamp) {
        forwardTimestampTo(timestamp);
        delegate.accumulateValue(newValue);
    }

    private void forwardTimestampTo(long timestamp) {
        latestTimestamp.accumulateAndGet(timestamp, Long::max);
    }

    public long getLatestTimestamp() {
        return latestTimestamp.get();
    }
}
