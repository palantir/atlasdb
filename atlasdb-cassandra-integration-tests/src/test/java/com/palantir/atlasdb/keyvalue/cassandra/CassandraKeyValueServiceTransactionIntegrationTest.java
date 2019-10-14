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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.ForwardingTransaction;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.WriteBatchingTransactionService;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.TimestampManagementService;

@ShouldRetry // The first test can fail with a TException: No host tried was able to create the keyspace requested.
@RunWith(Parameterized.class)
public class CassandraKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTest {
    private static final String SYNC = "sync";
    private static final String ASYNC = "async";
    private static final Supplier<KeyValueService> KVS_SUPPLIER =
            Suppliers.memoize(CassandraKeyValueServiceTransactionIntegrationTest::createAndRegisterKeyValueService);

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                {SYNC, (Function<Transaction, Transaction>) SynchronousDelegate::new},
                {ASYNC, (Function<Transaction, Transaction>) AsyncDelegate::new}
        };
        return Arrays.asList(data);
    }

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource(KVS_SUPPLIER);

    // This constant exists so that fresh timestamps are always greater than the write timestamps of values used in the
    // test.
    private static final long ONE_BILLION = 1_000_000_000;

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();
    private final String name;
    private final Function<Transaction, Transaction> transactionWrapper;

    public CassandraKeyValueServiceTransactionIntegrationTest(
            String name,
            Function<Transaction, Transaction> transactionWrapper) {
        super(CASSANDRA, CASSANDRA);
        this.name = name;
        this.transactionWrapper = transactionWrapper;
    }

    @Before
    public void advanceTimestamp() {
        ((TimestampManagementService) timestampService).fastForwardTimestamp(ONE_BILLION);
    }

    @Test
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

    private void tryPutTimestampPermittingExceptions(
            TransactionService transactionService,
            long timestamp) {
        try {
            transactionService.putUnlessExists(timestamp, timestamp + 1);
        } catch (KeyAlreadyExistsException ex) {
            // OK
        }
    }

    private static KeyValueService createAndRegisterKeyValueService() {
        CassandraKeyValueService kvs = CassandraKeyValueServiceImpl.createForTesting(CASSANDRA.getConfig());
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

    private static class SynchronousDelegate extends ForwardingTransaction {
        private final Transaction delegate;

        SynchronousDelegate(Transaction transaction) {
            this.delegate = transaction;
        }

        @Override
        public Transaction delegate() {
            return delegate;
        }

        @Override
        public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
            return delegate.get(tableRef, cells);
        }
    }

    private static class AsyncDelegate extends ForwardingTransaction {
        private final Transaction delegate;

        AsyncDelegate(Transaction transaction) {
            this.delegate = transaction;
        }

        @Override
        public Transaction delegate() {
            return delegate;
        }

        @Override
        public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
            try {
                return delegate.getAsync(tableRef, cells).get();
            } catch (InterruptedException | ExecutionException e) {
                throw com.palantir.common.base.Throwables.rewrapAndThrowUncheckedException(e.getCause());
            }
        }
    }
}
