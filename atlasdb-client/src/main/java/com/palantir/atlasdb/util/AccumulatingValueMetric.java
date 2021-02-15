/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import java.util.concurrent.atomic.AtomicLong;

public class AccumulatingValueMetric implements Gauge<Long> {
    private final AtomicLong value = new AtomicLong(0L);

    @Override
    public Long getValue() {
        return value.get();
    }

    public void setValue(Long newValue) {
        value.set(newValue);
    }

    public void accumulateValue(Long newValue) {
        value.addAndGet(newValue);
    }

    public void increment() {
        value.incrementAndGet();
    }
}
