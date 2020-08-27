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

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable.KvRowsRow;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable.KvRowsRowResult;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import java.util.List;
import java.util.Map;

public final class RangeScanRowsBenchmark extends AbstractRangeScanBenchmark {

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients,
            int requestsPerClient, int dataSize, int numRows) {
        return new RangeScanRowsBenchmark(txnManager, numClients, requestsPerClient, dataSize, numRows).execute();
    }

    private RangeScanRowsBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int dataSize, int numRows) {
        super(numClients, requestsPerClient, txnManager, dataSize, numRows);
    }

    @Override
    protected void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey) {
        KvRowsTable table = BenchmarksTableFactory.of().getKvRowsTable(txn);

        valuesByKey.forEach((key, value) -> {
            table.putData(KvRowsRow.of(bucket, key), value);
        });
    }

    @Override
    protected List<byte[]> getRange(Transaction txn, long startInclusive, long endExclusive) {
        KvRowsTable table = BenchmarksTableFactory.of().getKvRowsTable(txn);

        return table.getRange(
                RangeRequest.builder()
                        .startRowInclusive(KvRowsRow.of(bucket, startInclusive))
                        .endRowExclusive(KvRowsRow.of(bucket, endExclusive))
                        .batchHint(batchSize)
                        .build())
                .hintBatchSize(batchSize)
                .transform(KvRowsRowResult::getData)
                .immutableCopy();
    }

}
