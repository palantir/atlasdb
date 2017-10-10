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

package com.palantir.atlasdb.monitoring;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Clock;
import com.codahale.metrics.Gauge;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public final class TimestampTrackers {
    private TimestampTrackers() {
        // Utility Class
    }

    private static <T> Gauge<T> createGauge(
            Duration interval,
            TransactionManager transactionManager,
            Function<TransactionManager, T> function) {
        return createGauge(Clock.defaultClock(), interval, transactionManager, function);
    }

    static <T> Gauge<T> createGauge(
            Clock clock,
            Duration interval,
            TransactionManager transactionManager,
            Function<TransactionManager, T> function) {
        return new CachedGauge<T>(clock, interval.toMillis(), TimeUnit.MILLISECONDS) {
            @Override
            protected T loadValue() {
                return function.apply(transactionManager);
            }
        };
    }
}
