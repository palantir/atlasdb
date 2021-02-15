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

import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable.BlobsRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.random.RandomBytes;
import java.util.List;
import java.util.Map;

public final class TransactionWriteRowsBenchmark extends AbstractWriteTransactionBenchmark {

    public static Map<String, Object> execute(
            TransactionManager txnManager, int numClients, int requestsPerClient, int numRows, int dataSize) {
        return new TransactionWriteRowsBenchmark(txnManager, numClients, requestsPerClient, numRows, dataSize)
                .execute();
    }

    private TransactionWriteRowsBenchmark(
            TransactionManager txnManager, int numClients, int requestsPerClient, int numRows, int dataSize) {
        super(txnManager, numClients, requestsPerClient, numRows, dataSize);
    }

    @Override
    protected void writeValues(Transaction txn, List<byte[]> values) {
        BlobsTable table = tableFactory.getBlobsTable(txn);

        for (byte[] value : values) {
            table.putData(BlobsRow.of(RandomBytes.ofLength(16)), value);
        }
    }
}
