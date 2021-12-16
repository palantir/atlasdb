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

import static org.mockito.Mockito.mock;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.sweep.queue.SweepQueueReader;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;
import com.palantir.atlasdb.transaction.impl.GetAsyncDelegate;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.TransactionSchemaVersionEnforcement;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.UnaryOperator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CassandraKvsSerializableTransactionTest extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private static final String SYNC_TRANSACTIONS_1 = "sync, transactions1";
    private static final String ASYNC_TRANSACTIONS_3 = "async, transactions3";

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
            {
                SYNC_TRANSACTIONS_1,
                UnaryOperator.identity(),
                TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION
            },
            {
                ASYNC_TRANSACTIONS_3,
                (UnaryOperator<Transaction>) GetAsyncDelegate::new,
                TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION
            }
        };
        return Arrays.asList(data);
    }

    private final UnaryOperator<Transaction> transactionWrapper;
    private final int transactionsSchemaVersion;

    public CassandraKvsSerializableTransactionTest(
            String name, UnaryOperator<Transaction> transactionWrapper, int transactionsSchemaVersion) {
        super(CASSANDRA, CASSANDRA);
        this.transactionWrapper = transactionWrapper;
        this.transactionsSchemaVersion = transactionsSchemaVersion;
    }

    @Before
    public void before() {
        keyValueService.truncateTable(AtlasDbConstants.COORDINATION_TABLE);
        TransactionSchemaVersionEnforcement.ensureTransactionsGoingForwardHaveSchemaVersion(
                transactionSchemaManager, timestampService, timestampManagementService, transactionsSchemaVersion);
    }

    @Override
    protected Transaction startTransaction() {
        return transactionWrapper.apply(super.startTransaction());
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterUninitialized() {
        return TargetedSweeper.createUninitializedForTest(() -> 128);
    }

    @Override
    protected MultiTableSweepQueueWriter getSweepQueueWriterInitialized() {
        return SweepQueue.createWriter(
                mock(TargetedSweepMetrics.class),
                keyValueService,
                timelockService,
                () -> 128,
                SweepQueueReader.DEFAULT_READ_BATCHING_RUNTIME_CONTEXT);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }
}
