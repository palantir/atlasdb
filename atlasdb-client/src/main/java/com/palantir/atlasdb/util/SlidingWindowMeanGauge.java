/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.util;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;

public class SlidingWindowMeanGauge implements Gauge<Double> {
    private final Histogram histogram;

    public SlidingWindowMeanGauge() {
        histogram = new Histogram(new SlidingTimeWindowArrayReservoir(1, TimeUnit.HOURS));
    }

    @Override
    public Double getValue() {
        return getSnapshot().getMean();
    }

    public void update(long entry) {
        histogram.update(entry);
    }

    @SuppressWarnings("VisibleForTestingPackagePrivate") // used in SweepMetricsAssert
    @VisibleForTesting
    public Snapshot getSnapshot() {
        return histogram.getSnapshot();
    }
}
