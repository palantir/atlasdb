/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SnapshotTransaction;
import com.palantir.atlasdb.util.MetricsManager;

public final class SnapshotTransactionMetricFactory {
    // We continue to use the SnapshotTransaction origin to avoid a breaking change in our metric names.
    private static final Class<SnapshotTransaction> DELEGATE_ORIGIN = SnapshotTransaction.class;

    private final MetricsManager metricsManager;
    private final TableLevelMetricsController tableLevelMetricsController;

    public SnapshotTransactionMetricFactory(
            MetricsManager metricsManager, TableLevelMetricsController tableLevelMetricsController) {
        this.metricsManager = metricsManager;
        this.tableLevelMetricsController = tableLevelMetricsController;
    }

    public Timer getTimer(String name) {
        return metricsManager.registerOrGetTimer(DELEGATE_ORIGIN, name);
    }

    public Histogram getHistogram(String name) {
        return metricsManager.registerOrGetHistogram(DELEGATE_ORIGIN, name);
    }

    public Histogram getHistogram(String name, TableReference tableRef) {
        return metricsManager.registerOrGetTaggedHistogram(
                DELEGATE_ORIGIN, name, metricsManager.getTableNameTagFor(tableRef));
    }

    public Counter getCounter(String name, TableReference tableRef) {
        return tableLevelMetricsController.createAndRegisterCounter(DELEGATE_ORIGIN, name, tableRef);
    }
}
