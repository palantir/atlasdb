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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvRowsTable.KvRowsRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class RowsRangeScanBenchmark extends AbstractRangeScanBenchmark {

    private static final Logger log = LoggerFactory.getLogger(RowsRangeScanBenchmark.class);

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize, int numUpdates, int numDeleted) {
        return new RowsRangeScanBenchmark(txnManager, numClients, requestsPerClient, numRows, dataSize, numUpdates, numDeleted)
                .execute();
    }

    private RowsRangeScanBenchmark(
            SerializableTransactionManager txnManager,
            int numClients,
            int requestsPerClient,
            int numRows,
            int dataSize,
            int numUpdates,
            int numDeleted) {
        super(numClients, requestsPerClient, txnManager, numRows, dataSize, numUpdates, numDeleted);
    }

    @Override
    protected void writeValues(Transaction txn, Map<Long, byte[]> valuesByKey) {
        KvRowsTable table = BenchmarksTableFactory.of().getKvRowsTable(txn);

        valuesByKey.forEach((key, value) -> {
            table.putData(KvRowsRow.of(bucket, key), value);
        });
    }

    @Override
    protected void deleteValues(Transaction txn, Set<Long> keys) {
        KvRowsTable table = BenchmarksTableFactory.of().getKvRowsTable(txn);

        keys.forEach(key -> {
            table.deleteData(KvRowsRow.of(bucket, key));
        });
    }

    @Override
    protected long getRange(Transaction txn, long startInclusive, long endExclusive) {
        log.info("getRange: {} to {}",
                PtBytes.encodeHexString(KvRowsRow.of(bucket, startInclusive).persistToBytes()),
                PtBytes.encodeHexString(KvRowsRow.of(bucket, endExclusive).persistToBytes()));
        KvRowsTable table = BenchmarksTableFactory.of().getKvRowsTable(txn);

        return table.getRange(
                RangeRequest.builder()
                        .startRowInclusive(KvRowsRow.of(bucket, startInclusive))
                        .endRowExclusive(KvRowsRow.of(bucket, endExclusive))
                        .batchHint(batchSize)
                        .build())
                .hintBatchSize(batchSize)
                .count();
    }

}
