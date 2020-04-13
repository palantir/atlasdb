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
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.palantir.atlasdb.v2.api.api.ConflictChecker;
import com.palantir.atlasdb.v2.api.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.api.NewLockToken;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.exception.FailedConflictCheckingException;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.kvs.DefaultConflictChecker;
import com.palantir.atlasdb.v2.api.kvs.LegacyKvs;
import com.palantir.atlasdb.v2.api.locks.LegacyLocks;
import com.palantir.atlasdb.v2.api.transaction.SingleThreadedTransaction;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReadReportingReader;
import com.palantir.atlasdb.v2.api.transaction.scanner.Reader;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderChain;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderFactory;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.v2.LeadershipId;

public class TestTxnManager<Txn> {
    private static final Logger log = LoggerFactory.getLogger(TestTxnManager.class);
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
    private final Reader<ReadReportingReader.RecordingNewValue> baseReader = ReaderChain.create(kvs)
            .then(readerFactory.readCommittedData())
            .then(readerFactory.mergeInTransactionWrites())
            .then(readerFactory.reportReads())
            .then(readerFactory.stopAfterMarker())
            .then(readerFactory.orderValidating())
            .then(readerFactory.checkImmutableLocks())
            .build();
    private final ConflictChecker conflictChecker = new DefaultConflictChecker(iterators, readerFactory);
    private final Function<NewTransaction, Txn> wrapper;

    public TestTxnManager(Function<NewTransaction, Txn> wrapper) {
        this.wrapper = wrapper;
    }

    private SingleThreadedTransaction createTransaction(
            Executor executor, long immutableTimestamp, long startTimestamp, NewLockToken immutableLock) {
        TransactionState state = TransactionState.newTransaction(
                executor, immutableTimestamp, startTimestamp, immutableLock);
        return new SingleThreadedTransaction(baseReader, kvs, locks, locks, conflictChecker, iterators, state);
    }

    public TestExecutor executor() {
        return executor;
    }

    public <T> ListenableFuture<T> callTransaction(AsyncFunction<Txn, T> task) {
        return Futures.catchingAsync(
                transactionAttempt(task),
                FailedConflictCheckingException.class,
                thrown -> {
                    log.info("Caught exception", thrown);
                    return callTransaction(task);
                },
                executor.soonScheduler());
    }

    private <T> ListenableFuture<T> transactionAttempt(AsyncFunction<Txn, T> task) {
        Executor scheduler = executor.nowScheduler();
        return FutureChain.start(scheduler, locks.lockImmutableTs())
                .defer(il -> locks.unlock(ImmutableSet.of(il.lockToken())))
                .then($ -> locks.getStartTimestamp(),
                        (il, ts) -> createTransaction(scheduler, il.immutableTimestamp(), ts, il.lockToken()))
                .defer(txn -> txn.end(NewEndOperation.ABORT)) // abort if nothing else
                .then(txn -> task.apply(wrapper.apply(txn)),
                        txn -> txn.end(NewEndOperation.COMMIT), ($, result) -> result)
                .done();
    }

    public ListenableFuture<?> runTransaction(Consumer<Txn> task) {
        return callTransaction(txn -> {
            task.accept(txn);
            return Futures.immediateFuture(null);
        });
    }

    private enum FakeSweepQueue implements MultiTableSweepQueueWriter {
        INSTANCE;

        @Override
        public void enqueue(List<WriteInfo> writes) {}
    }

}
