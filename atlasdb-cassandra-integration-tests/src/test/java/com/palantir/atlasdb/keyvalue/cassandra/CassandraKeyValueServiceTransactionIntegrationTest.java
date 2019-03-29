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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.encoding.TicketsEncodingStrategy;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.WriteBatchingTransactionService;
import com.palantir.exception.NotInitializedException;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.TimestampManagementService;

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
    public void f178923401274891() {
        TransactionTables.createTables(keyValueService);
        TransactionService s = SimpleTransactionService.createV2(keyValueService);
        s.putUnlessExists(15, 666);
        keyValueService.putUnlessExists(
                TransactionConstants.TRANSACTIONS2_TABLE,
                ImmutableMap.<Cell, byte[]>builder()
                .put(TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(31),
                        TicketsEncodingStrategy.INSTANCE.encodeCommitTimestampAsValue(31, 999))
                .build());

    }

    @Test
    public void f1234567() {
        TransactionTables.createTables(keyValueService);
        TransactionTables.truncateTables(keyValueService);
        TransactionService s = WriteBatchingTransactionService.create(
                SimpleTransactionService.createV2(keyValueService));
        ExecutorService es = Executors.newCachedThreadPool();
        List<Future<?>> f = LongStream.range(0, 100)
                .mapToObj(unused -> {
                    return es.submit(() -> {
                        try {
                            s.putUnlessExists(unused, unused + 1);
                            s.putUnlessExists(unused + 1, unused + 3);
                            s.putUnlessExists(unused - 1, unused + 5);
                            s.putUnlessExists(unused + 2, unused + 7);
                            s.putUnlessExists(unused - 2, unused + 9);
                        } catch (KeyAlreadyExistsException ex) {
                            // bleh
                            System.out.println(ex.getExistingKeys());
                        }
                    });
                })
                .collect(Collectors.toList());
        f.forEach(Futures::getUnchecked);
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
