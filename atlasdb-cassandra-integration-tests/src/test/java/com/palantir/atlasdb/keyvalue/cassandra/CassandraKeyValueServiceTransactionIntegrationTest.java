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
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.WriteBatchingTransactionService;
import com.palantir.exception.NotInitializedException;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.TimestampManagementService;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

@ShouldRetry // The first test can fail with a TException: No host tried was able to create the keyspace requested.
public class CassandraKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTest {
    private final Supplier<KeyValueService> kvsSupplier = Suppliers.memoize(this::createAndRegisterKeyValueService);

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    // This constant exists so that fresh timestamps are always greater than the write timestamps of values used in the
    // test.
    private static final long ONE_BILLION = 1_000_000_000;

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    public CassandraKeyValueServiceTransactionIntegrationTest() {
        super(CASSANDRA, CASSANDRA);
    }

    @Before
    public void advanceTimestamp() {
        ((TimestampManagementService) timestampService).fastForwardTimestamp(ONE_BILLION);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return kvsSupplier.get();
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

    private KeyValueService createAndRegisterKeyValueService() {
        KeyValueService kvs = CassandraKeyValueServiceImpl.create(
                metricsManager,
                CASSANDRA.getConfig(),
                CassandraMutationTimestampProviders.singleLongSupplierBacked(
                        () -> {
                            if (timestampService == null) {
                                throw new NotInitializedException("timestamp service");
                            }
                            return timestampService.getFreshTimestamp();
                        }));
        CASSANDRA.registerKvs(kvs);
        return kvs;
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
