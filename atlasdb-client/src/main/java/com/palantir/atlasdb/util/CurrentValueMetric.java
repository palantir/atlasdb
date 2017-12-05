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

package com.palantir.atlasdb.util;

import com.codahale.metrics.Gauge;

public class CurrentValueMetric implements Gauge<Long> {
    volatile long value = 0;

    @Override
    public Long getValue() {
        return value;
    }

    public void setValue(long newValue) {
        value = newValue;
    }

    public static class MaximumValueMetric extends CurrentValueMetric {
        @Override
        public void setValue(long newValue) {
            synchronized (this) {
                super.setValue(Math.max(getValue(), newValue));
            }
        }
    }
}
