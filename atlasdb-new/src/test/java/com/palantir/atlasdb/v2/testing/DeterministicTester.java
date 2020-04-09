/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.v2.testing;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.WriteInfo;
import com.palantir.atlasdb.timelock.lock.LeaderClock;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.v2.api.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Column;
import com.palantir.atlasdb.v2.api.api.NewIds.Row;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.api.NewTransactionManager;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.ConflictChecker;
import com.palantir.atlasdb.v2.api.kvs.DefaultConflictChecker;
import com.palantir.atlasdb.v2.api.kvs.LegacyKvs;
import com.palantir.atlasdb.v2.api.locks.LegacyLocks;
import com.palantir.atlasdb.v2.api.api.NewLockToken;
import com.palantir.atlasdb.v2.api.transaction.SingleThreadedTransaction;
import com.palantir.atlasdb.v2.api.transaction.scanner.PostFilterWritesReader.ShouldAbortWrites;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReadReportingReader.RecordingNewValue;
import com.palantir.atlasdb.v2.api.transaction.scanner.Reader;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderChain;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderFactory;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeadershipId;

public class DeterministicTester {
    private static final LeadershipId LEADERSHIP_ID = LeadershipId.random();
    private final TestExecutor executor = new TestExecutor();
    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final LegacyKvs kvs = new LegacyKvs(
            executor.soonScheduler(),
            SimpleTransactionService.createV1(keyValueService),
            keyValueService,
            FakeSweepQueue.INSTANCE,
            new DefaultTimestampCache(new MetricRegistry(), () -> 100_000L));
    private final LegacyLocks locks = new LegacyLocks(executor.actuallyProgrammableScheduler(),
            new LeaderClock(LEADERSHIP_ID, () -> NanoTime.createForTests(0)));
    private final AsyncIterators iterators = new AsyncIterators(executor.nowScheduler());
    private final ReaderFactory readerFactory = new ReaderFactory(iterators, kvs, locks, locks);
    private final Reader<RecordingNewValue> baseReader = ReaderChain.create(kvs)
            .then(readerFactory.postFilterWrites(ShouldAbortWrites.YES))
            .then(readerFactory.mergeInTransactionWrites())
            .then(readerFactory.reportReads())
            .then(readerFactory.stopAfterMarker())
            .then(readerFactory.orderValidating())
            .then(readerFactory.checkImmutableLocks())
            .build();
    private final ConflictChecker conflictChecker = new DefaultConflictChecker(iterators, readerFactory);
    private final TestTransactionManager txnManager = new TestTransactionManager();

    private static final Table TABLE = NewIds.table("table.table1");
    private static final Row ROW = NewIds.row(new byte[100]);
    private static final Column COLUMN = NewIds.column(new byte[200]);
    private static final Cell CELL = NewIds.cell(ROW, COLUMN);

    private SingleThreadedTransaction createTransaction(
            Executor executor, long immutableTimestamp, long startTimestamp, NewLockToken immutableLock) {
        TransactionState state = TransactionState.newTransaction(
                executor, immutableTimestamp, startTimestamp, immutableLock);
        return new SingleThreadedTransaction(baseReader, kvs, locks, locks, conflictChecker, iterators, state);
    }

    private final class TestTransactionManager implements NewTransactionManager {
        @Override
        public <T> ListenableFuture<T> executeTransaction(AsyncFunction<NewTransaction, T> task) {
            Executor scheduler = executor.nowScheduler();
            return FutureChain.start(scheduler, locks.lockImmutableTs())
                    .defer(il -> locks.unlock(ImmutableSet.of(il.lockToken())))
                    .then($ -> locks.getStartTimestamp(),
                            (il, ts) -> createTransaction(scheduler, il.immutableTimestamp(), ts, il.lockToken()))
                    .defer(txn -> txn.end(NewEndOperation.ABORT)) // abort if nothing else
                    .then(task::apply, txn -> txn.end(NewEndOperation.COMMIT), ($, result) -> result)
                    .done();
        }
    }

    @Test
    public void testExecutor() {
        ListenableFuture<?> result = txnManager.executeTransaction(txn -> {
            txn.put(NewPutOperation.of(TABLE, CELL, Optional.of(NewIds.value(new byte[12345]))));
            return Futures.immediateFuture(null);
        });
        executor.start();
        Futures.getUnchecked(result);
    }

    private enum FakeSweepQueue implements MultiTableSweepQueueWriter {
        INSTANCE;

        @Override
        public void enqueue(List<WriteInfo> writes) {}
    }
}
