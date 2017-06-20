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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;

public class ContendedWriteTransactionBenchmark extends AbstractBenchmark {

    private static final Logger log = LoggerFactory.getLogger(WriteTransactionBenchmark.class);

    private static final TableReference TABLE1 = TableReference.create(
            Namespace.create(ContendedWriteTransactionBenchmark.class.getSimpleName()),
            "table1");
    private static final TableReference TABLE2 = TableReference.create(
            Namespace.create(ContendedWriteTransactionBenchmark.class.getSimpleName()),
            "table2");
    private static final byte[] METADATA = new TableMetadata(new NameMetadataDescription(),
            new ColumnMetadataDescription(),
            ConflictHandler.SERIALIZABLE).persistToBytes();

    private final TransactionManager txnManager;
    private final byte[] key;
    private final byte[] originalValue;

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient) {
        txnManager.getKeyValueService().createTable(TABLE1, METADATA);
        txnManager.getKeyValueService().createTable(TABLE2, METADATA);

        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] originalValue = UUID.randomUUID().toString().getBytes();
        txnManager.runTaskWithRetry(txn -> {
            txn.put(TABLE1, ImmutableMap.of(Cell.create(key, key), originalValue));
            txn.put(TABLE2, ImmutableMap.of(Cell.create(key, key), originalValue));
            return null;
        });

        return new ContendedWriteTransactionBenchmark(txnManager, key, originalValue, numClients, requestsPerClient).execute();
    }

    private ContendedWriteTransactionBenchmark(TransactionManager txnManager, byte[] key, byte[] originalValue, int numClients, int requestsPerClient) {
        super(numClients, requestsPerClient);

        this.txnManager = txnManager;
        this.key = key;
        this.originalValue = originalValue;
    }

    @Override
    public void performOneCall() {
        runContendedTransaction(TABLE1);
        runContendedTransaction(TABLE2);
    }

    private void runContendedTransaction(TableReference table) {
        txnManager.runTaskWithRetry(txn -> {
            Cell cell = Cell.create(key, key);
            byte[] currentValue = txn.get(table, ImmutableSet.of(cell)).get(cell);

            if (Arrays.equals(currentValue, originalValue)) {
                byte[] newValue = UUID.randomUUID().toString().getBytes();
                txn.put(table, ImmutableMap.of(cell, newValue));
            }

            return null;
        });
    }

}
