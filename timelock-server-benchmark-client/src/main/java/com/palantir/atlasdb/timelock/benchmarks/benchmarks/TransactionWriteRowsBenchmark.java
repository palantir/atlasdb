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

import java.util.List;
import java.util.Map;

import com.palantir.atlasdb.timelock.benchmarks.RandomBytes;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable;
import com.palantir.atlasdb.timelock.benchmarks.schema.generated.BlobsTable.BlobsRow;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public final class TransactionWriteRowsBenchmark extends AbstractWriteTransactionBenchmark {

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient, int numRows, int dataSize) {
        return new TransactionWriteRowsBenchmark(txnManager, numClients, requestsPerClient, numRows,
                dataSize).execute();
    }

    private TransactionWriteRowsBenchmark(TransactionManager txnManager, int numClients, int requestsPerClient,
            int numRows, int dataSize) {
        super(txnManager, numClients, requestsPerClient, numRows, dataSize);
    }

    @Override
    protected void writeValues(Transaction txn, List<byte[]> values) {
        BlobsTable table = tableFactory.getBlobsTable(txn);

        for (byte[] value : values) {
            table.putData(
                    BlobsRow.of(RandomBytes.ofLength(16)),
                    value);
        }
    }

}
