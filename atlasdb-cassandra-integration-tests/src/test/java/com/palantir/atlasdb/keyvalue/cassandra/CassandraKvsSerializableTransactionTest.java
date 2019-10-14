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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.metrics.TargetedSweepMetrics;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;
import com.palantir.atlasdb.transaction.impl.ForwardingTransaction;

@RunWith(Parameterized.class)
public class CassandraKvsSerializableTransactionTest extends AbstractSerializableTransactionTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                {"sync", (Function<Transaction, Transaction>) SynchronousDelegate::new},
                {"async", (Function<Transaction, Transaction>) AsyncDelegate::new}
        };
        return Arrays.asList(data);
    }

    private final Function<Transaction, Transaction> transactionWrapper;

    public CassandraKvsSerializableTransactionTest(
            String name,
            Function<Transaction, Transaction> transactionWrapper) {
        super(CASSANDRA, CASSANDRA);
        this.transactionWrapper = transactionWrapper;
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
                () -> 1);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
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
