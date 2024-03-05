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

import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.AtlasDbMetricNames.CellFilterMetrics;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;

public final class KeyValueSnapshotEventRecorder {
    // This dichotomy is unfortunate, but a result of us standing between the legacy and metric-schema worlds.
    private final SnapshotTransactionMetricFactory metricFactory;
    private final TransactionMetrics transactionMetrics;

    private KeyValueSnapshotEventRecorder(
            SnapshotTransactionMetricFactory metricFactory, TransactionMetrics transactionMetrics) {
        this.metricFactory = metricFactory;
        this.transactionMetrics = transactionMetrics;
    }

    public static KeyValueSnapshotEventRecorder create(
            MetricsManager metricsManager, TableLevelMetricsController tableLevelMetricsController) {
        return new KeyValueSnapshotEventRecorder(
                new SnapshotTransactionMetricFactory(metricsManager, tableLevelMetricsController),
                TransactionMetrics.of(metricsManager.getTaggedRegistry()));
    }

    public void recordCellsRead(TableReference tableReference, long cellsRead) {
        metricFactory
                .getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_READ, tableReference)
                .inc(cellsRead);
    }

    public void recordCellsReturned(TableReference tableReference, long cellsReturned) {
        metricFactory
                .getCounter(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_CELLS_RETURNED, tableReference)
                .inc(cellsReturned);
    }

    public void recordManyBytesReadForTable(TableReference tableReference, long bytesRead) {
        metricFactory
                .getHistogram(AtlasDbMetricNames.SNAPSHOT_TRANSACTION_TOO_MANY_BYTES_READ, tableReference)
                .update(bytesRead);
    }

    public void recordFilteredSweepSentinel(TableReference tableReference) {
        metricFactory
                .getCounter(CellFilterMetrics.INVALID_START_TS, tableReference)
                .inc();
    }

    public void recordFilteredUncommittedTransaction(TableReference tableReference) {
        metricFactory
                .getCounter(CellFilterMetrics.INVALID_COMMIT_TS, tableReference)
                .inc();
    }

    public void recordFilteredTransactionCommittingAfterOurStart(TableReference tableReference) {
        metricFactory
                .getCounter(CellFilterMetrics.COMMIT_TS_GREATER_THAN_TRANSACTION_TS, tableReference)
                .inc();
    }

    public void recordRolledBackOtherTransaction() {
        transactionMetrics.rolledBackOtherTransaction().mark();
    }
}
