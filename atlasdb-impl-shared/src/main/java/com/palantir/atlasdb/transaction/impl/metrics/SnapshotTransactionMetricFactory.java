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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.impl.SnapshotTransaction;
import com.palantir.atlasdb.util.MetricsManager;

public final class SnapshotTransactionMetricFactory {
    private final MetricsManager metricsManager;
    private final TableLevelMetricsController tableLevelMetricsController;

    public SnapshotTransactionMetricFactory(
            MetricsManager metricsManager, TableLevelMetricsController tableLevelMetricsController) {
        this.metricsManager = metricsManager;
        this.tableLevelMetricsController = tableLevelMetricsController;
    }

    // Using the SnapshotTransaction class and not this class, to preserve backwards compatibility
    public Histogram getHistogram(String name, TableReference tableRef) {
        return metricsManager.registerOrGetTaggedHistogram(
                SnapshotTransaction.class, name, metricsManager.getTableNameTagFor(tableRef));
    }

    public Counter getCounter(String name, TableReference tableRef) {
        return tableLevelMetricsController.createAndRegisterCounter(SnapshotTransaction.class, name, tableRef);
    }
}
