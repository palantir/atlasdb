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
package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumn;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumnValue;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RangeScanDynamicColumnsBenchmark extends AbstractRangeScanBenchmark {

    public static Map<String, Object> execute(
            TransactionManager txnManager, int numClients, int requestsPerClient, int dataSize, int numRows) {
        return new RangeScanDynamicColumnsBenchmark(txnManager, numClients, requestsPerClient, dataSize, numRows)
                .execute();
    }

    private RangeScanDynamicColumnsBenchmark(
            TransactionManager txnManager, int numClients, int requestsPerClient, int dataSize, int numRows) {
        super(numClients, requestsPerClient, txnManager, dataSize, numRows);
    }

    @Override
    protected void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey) {
        KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

        valuesByKey.forEach((key, value) -> {
            table.put(
                    KvDynamicColumnsRow.of(bucket),
                    KvDynamicColumnsColumnValue.of(KvDynamicColumnsColumn.of(key), value));
        });
    }

    @Override
    protected List<byte[]> getRange(Transaction txn, long startInclusive, long endExclusive) {
        KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

        List<byte[]> data = new ArrayList<>();
        table.getRowsColumnRange(
                        ImmutableSet.of(KvDynamicColumnsRow.of(bucket)),
                        new ColumnRangeSelection(
                                KvDynamicColumnsColumn.of(startInclusive).persistToBytes(),
                                KvDynamicColumnsColumn.of(endExclusive).persistToBytes()),
                        batchSize)
                .forEachRemaining(entry -> data.add(entry.getValue().getValue()));

        return data;
    }
}
