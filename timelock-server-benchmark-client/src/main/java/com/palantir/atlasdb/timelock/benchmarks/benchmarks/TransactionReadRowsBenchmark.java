/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable.BlobsRow;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class TransactionReadRowsBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(TransactionReadRowsBenchmark.class);

    private final TransactionManager txnManager;

    private final int dataSize;
    private final List<byte[]> keys;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize) {
        return new TransactionReadRowsBenchmark(txnManager, numClients, requestsPerClient, numRows, dataSize).execute();
    }

    private TransactionReadRowsBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int numRows, int dataSize) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;

        this.keys = IntStream.range(0, numRows)
                .mapToObj(i -> RandomBytes.ofLength(16))
                .collect(Collectors.toList());
        this.dataSize = dataSize;
    }

    @Override
    public void setup() {
        txnManager.runTaskWithRetry(txn -> {
            BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
            for (byte[] key : keys) {
                table.putData(BlobsRow.of(key), RandomBytes.ofLength(dataSize));
            }
            return null;
        });
    }

    @Override
    public void performOneCall() {
        List<byte[]> result = txnManager.runTaskReadOnly(txn -> {
            BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
            List<BlobsRow> rowKeys = keys.stream().map(BlobsRow::of).collect(Collectors.toList());

            return table.getRows(rowKeys).stream()
                    .map(BlobsTable.BlobsRowResult::getData)
                    .collect(Collectors.toList());
        });

        Preconditions.checkState(result.size() == keys.size());
    }

}
