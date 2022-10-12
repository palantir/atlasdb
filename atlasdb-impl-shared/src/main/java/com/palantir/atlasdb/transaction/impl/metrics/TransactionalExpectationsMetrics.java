/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;

public class TransactionalExpectationsMetrics {
    private static final String JAVA_VERSION = System.getProperty("java.version", "unknown");

    private static final String LIBRARY_NAME = "atlasdb";

    private static final String LIBRARY_VERSION = Objects.requireNonNullElse(
            TransactionalExpectationsMetrics.class.getPackage().getImplementationVersion(), "unknown");
    private final MetricsManager manager;

    private final LongAccumulator maximumBytesReadAcrossTransactions = new LongAccumulator(Long::max, 0L);

    public TransactionalExpectationsMetrics(MetricsManager manager) {
        this.manager = manager;
    }

    private void registerMetrics() {

        MetricName metricName = MetricName.builder()
            .safeName("transactionalExpectationsMetrics.maximumBytesReadAcrossTransactionsInOneTransactionManager")
            .putSafeTags("libraryName", LIBRARY_NAME)
            .putSafeTags("libraryVersion", LIBRARY_VERSION)
            .putSafeTags("javaVersion", JAVA_VERSION)
            .build();

        manager.getTaggedRegistry().registerWithReplacement(metricName, );
    }

    private void registerMetricsFilter() {
        return;
    }
}

class RunningMaximum implements Gauge<Long> {
    private final LongAccumulator value = new LongAccumulator(Long::max, 0L);
    @Override
    public Long getValue() {
        return value.get();
    }

    public void update(long newValue) {
        value.accumulate(newValue);
    }
}
