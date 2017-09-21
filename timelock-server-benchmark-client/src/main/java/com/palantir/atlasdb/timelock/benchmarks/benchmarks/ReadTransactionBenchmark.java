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
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BenchmarksTableFactory;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable.BlobsRow;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class ReadTransactionBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(WriteTransactionBenchmark.class);

    private final TransactionManager txnManager;

    private final String key = UUID.randomUUID().toString();
    private final byte[] data = RandomBytes.ofLength(16);

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient) {
        return new ReadTransactionBenchmark(txnManager, numClients, requestsPerClient).execute();
    }

    private ReadTransactionBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient) {
        super(numClients, requestsPerClient);
        this.txnManager = txnManager;
    }

    @Override
    public void setup() {
        txnManager.runTaskWithRetry(txn -> {
            BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
            table.putData(BlobsRow.of(data), data);
            return null;
        });
    }

    @Override
    public void performOneCall() {
        byte[] result = txnManager.runTaskReadOnly(txn -> {
            BlobsTable table = BenchmarksTableFactory.of().getBlobsTable(txn);
            return table.getRow(BlobsRow.of(data)).get().getData();
        });

        Preconditions.checkState(result != null);
    }

}
