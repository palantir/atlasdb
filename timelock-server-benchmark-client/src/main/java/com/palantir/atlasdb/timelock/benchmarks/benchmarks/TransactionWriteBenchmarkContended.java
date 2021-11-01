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

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsSerializableTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsSerializableTable.BlobsSerializableRow;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.random.RandomBytes;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public final class TransactionWriteBenchmarkContended extends AbstractBenchmark {

    private static final SafeLogger log = SafeLoggerFactory.get(TransactionWriteBenchmarkContended.class);

    private static final BenchmarksTableFactory tableFactory = BenchmarksTableFactory.of();

    private final TransactionManager txnManager;
    private final Map<byte[], byte[]> originalValuesByKey;

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients, int requestsPerClient) {
        return new TransactionWriteBenchmarkContended(txnManager, numClients, requestsPerClient).execute();
    }

    private TransactionWriteBenchmarkContended(TransactionManager txnManager, int numClients, int requestsPerClient) {
        super(numClients, 1);
        originalValuesByKey = LongStream.range(0, requestsPerClient)
                .boxed()
                .collect(Collectors.toMap(PtBytes::toBytes, _ignore -> RandomBytes.ofLength(16)));
        this.txnManager = txnManager;
    }

    @Override
    public void setup() {
        txnManager.runTaskWithRetry(txn -> {
            BlobsSerializableTable table = tableFactory.getBlobsSerializableTable(txn);
            originalValuesByKey.forEach((key, value) -> table.putData(BlobsSerializableRow.of(key), value));
            return null;
        });
    }

    @Override
    public void performOneCall() {
        originalValuesByKey.forEach(this::runContendedTransaction);
    }

    private void runContendedTransaction(byte[] key, byte[] originalValue) {
        txnManager.runTaskWithRetry(txn -> {
            BlobsSerializableTable table = tableFactory.getBlobsSerializableTable(txn);

            byte[] currentValue =
                    table.getRow(BlobsSerializableRow.of(key)).get().getData();

            if (Arrays.equals(currentValue, originalValue)) {
                byte[] newValue = RandomBytes.ofLength(16);
                table.putData(BlobsSerializableRow.of(key), newValue);
            }

            return null;
        });
    }

    @Override
    protected void cleanup() {
        txnManager
                .getKeyValueService()
                .truncateTable(tableFactory.getBlobsSerializableTable(null).getTableRef());
    }
}
