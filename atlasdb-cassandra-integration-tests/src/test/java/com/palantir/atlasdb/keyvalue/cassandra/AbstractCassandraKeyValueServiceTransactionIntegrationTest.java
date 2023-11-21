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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTestV2;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.WriteBatchingTransactionService;
import com.palantir.flake.FlakeRetryTest;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

public abstract class AbstractCassandraKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTestV2 {

    private static final Supplier<KeyValueService> KVS_SUPPLIER = Suppliers.memoize(
            AbstractCassandraKeyValueServiceTransactionIntegrationTest::createAndRegisterKeyValueService);

    @RegisterExtension
    public static final CassandraResource CASSANDRA = new CassandraResource(KVS_SUPPLIER);

    // This constant exists so that fresh timestamps are always greater than the write timestamps of values used in the
    // test.
    private static final long ONE_BILLION = 1_000_000_000;

    private final UnaryOperator<Transaction> transactionWrapper;
    private final int transactionsSchemaVersion;

    public AbstractCassandraKeyValueServiceTransactionIntegrationTest(
            UnaryOperator<Transaction> transactionWrapper, int transactionsSchemaVersion) {
        super(CASSANDRA, CASSANDRA);
        this.transactionWrapper = transactionWrapper;
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @BeforeEach
    public void before() {
        advanceTimestamp();
        installTransactionsVersion();
    }

    private void advanceTimestamp() {
        timestampManagementService.fastForwardTimestamp(ONE_BILLION);
    }

    private void installTransactionsVersion() {
        keyValueService.truncateTable(AtlasDbConstants.COORDINATION_TABLE);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                transactionSchemaManager, timestampService, timestampManagementService, transactionsSchemaVersion);
    }

    @FlakeRetryTest
    public void canUntangleHighlyConflictingPutUnlessExists() {
        TransactionTables.createTables(keyValueService);
        TransactionTables.truncateTables(keyValueService);
        SimpleTransactionService v2TransactionService = SimpleTransactionService.createV2(keyValueService);
        TransactionService transactionService = WriteBatchingTransactionService.create(v2TransactionService);
        ExecutorService executorService = Executors.newCachedThreadPool();

        int numTransactionPutters = 100;
        List<Future<?>> futures = LongStream.range(1, numTransactionPutters)
                .mapToObj(timestamp -> executorService.submit(() -> {
                    tryPutTimestampPermittingExceptions(transactionService, timestamp);
                    tryPutTimestampPermittingExceptions(transactionService, timestamp + 1);
                }))
                .collect(Collectors.toList());
        futures.forEach(Futures::getUnchecked);
    }

    private void tryPutTimestampPermittingExceptions(TransactionService transactionService, long timestamp) {
        try {
            transactionService.putUnlessExists(timestamp, timestamp + 1);
        } catch (KeyAlreadyExistsException ex) {
            // OK
        }
    }

    private static KeyValueService createAndRegisterKeyValueService() {
        CassandraKeyValueService kvs =
                CassandraKeyValueServiceImpl.createForTesting(CASSANDRA.getConfig(), CASSANDRA.getRuntimeConfig());
        CASSANDRA.registerKvs(kvs);
        return kvs;
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

    @Override
    protected Transaction startTransaction() {
        return transactionWrapper.apply(super.startTransaction());
    }
}
