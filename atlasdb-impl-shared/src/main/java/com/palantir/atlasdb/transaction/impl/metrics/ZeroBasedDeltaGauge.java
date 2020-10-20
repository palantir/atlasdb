/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.transaction.impl.metrics;

import com.codahale.metrics.Gauge;
import java.util.function.LongSupplier;

/**
 * Reports the difference between measurements of a supplier of long values.
 *
 * The first value is returned as is. Users of this gauge should be very careful when attempting to evaluate deltas of
 * sequences that do not start at zero, as they will on their first measurement not give an accurate delta.
 */
public class ZeroBasedDeltaGauge implements Gauge<Long> {
    private final LongSupplier underlying;

    private long previousValue = 0;

    public ZeroBasedDeltaGauge(LongSupplier underlying) {
        this.underlying = underlying;
    }

    @Override
    public synchronized Long getValue() {
        long currentValue = underlying.getAsLong();
        long previous = previousValue;
        previousValue = currentValue;
        return currentValue - previous;
    }
}
