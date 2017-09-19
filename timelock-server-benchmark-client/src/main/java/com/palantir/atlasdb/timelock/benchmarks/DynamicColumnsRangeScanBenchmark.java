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

package com.palantir.atlasdb.timelock.benchmarks;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.api.ColumnRangeSelection;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumn;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsColumnValue;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.KvDynamicColumnsTable.KvDynamicColumnsRow;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class DynamicColumnsRangeScanBenchmark extends AbstractBenchmark {

    private final SerializableTransactionManager txnManager;

    private final String bucket;
    private final byte[] data;
    private final int numRows;
    private final int batchSize;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int dataSize, int numRows) {
        return new DynamicColumnsRangeScanBenchmark(txnManager, numClients, requestsPerClient, dataSize,
                numRows).execute();
    }

    private DynamicColumnsRangeScanBenchmark(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient,
            int dataSize, int numRows) {
        super(numClients, requestsPerClient);

        this.txnManager = txnManager;

        this.bucket = UUID.randomUUID().toString();
        this.numRows = numRows;
        this.data = randomDataOfLength(dataSize);
        this.batchSize = Math.max(1, 10_000_000 / dataSize);

        writeData();
    }

    private void writeData() {
        int numWritten = 0;
        AtomicLong key = new AtomicLong(0);
        while (numWritten < numRows) {
            int numToWrite = Math.min(numRows - numWritten, batchSize);
            txnManager.runTaskWithRetry(txn -> {
                KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

                for (int i = 0; i < numToWrite; i++) {
                    table.put(
                            KvDynamicColumnsRow.of(bucket),
                            KvDynamicColumnsColumnValue.of(KvDynamicColumnsColumn.of(key.getAndIncrement()), data));
                }

                return null;
            });
            numWritten += numToWrite;
        }
    }

    private byte[] randomDataOfLength(int dataSize) {
        byte[] result = new byte[dataSize];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    @Override
    protected void performOneCall() {
        List<byte[]> results = txnManager.runTaskReadOnly(txn -> {
            KvDynamicColumnsTable table = BenchmarksTableFactory.of().getKvDynamicColumnsTable(txn);

            List<byte[]> data = Lists.newArrayList();
            table.getRowsColumnRange(
                    ImmutableSet.of(KvDynamicColumnsRow.of(bucket)),
                    new ColumnRangeSelection(
                            KvDynamicColumnsColumn.of(0L).persistToBytes(),
                            KvDynamicColumnsColumn.of(numRows).persistToBytes()),
                    batchSize)
                    .forEachRemaining(entry -> data.add(entry.getValue().getValue()));

            return data;
        });

        Preconditions.checkState(results.size() == numRows);
    }
}
