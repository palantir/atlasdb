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

package com.palantir.atlasdb.v2.api.transaction;

import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.AsyncIterators;
import com.palantir.atlasdb.v2.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.NewGetOperation;
import com.palantir.atlasdb.v2.api.NewGetOperation.ResultBuilder;
import com.palantir.atlasdb.v2.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.NewTransaction;
import com.palantir.atlasdb.v2.api.NewValue;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.future.FutureChain.Tag;
import com.palantir.atlasdb.v2.api.kvs.ConflictChecker;
import com.palantir.atlasdb.v2.api.kvs.Writer;
import com.palantir.atlasdb.v2.api.locks.NewLocks;
import com.palantir.atlasdb.v2.api.timestamps.Timestamps;
import com.palantir.atlasdb.v2.api.util.Unreachable;

public class SingleThreadedTransaction implements NewTransaction {
    private final Scanner<NewValue> scanner;
    private final Writer writer;
    private final NewLocks locks;
    private final ConflictChecker conflictChecker;
    private final AsyncIterators iterators;
    private final Timestamps timestamps;
    private TransactionState state;

    public SingleThreadedTransaction(
            Scanner<NewValue> scanner,
            Writer writer, NewLocks locks,
            ConflictChecker conflictChecker,
            AsyncIterators iterators, Timestamps timestamps) {
        this.scanner = scanner;
        this.writer = writer;
        this.locks = locks;
        this.conflictChecker = conflictChecker;
        this.iterators = iterators;
        this.timestamps = timestamps;
    }

    @Override
    public void put(NewPutOperation put) {
        checkNotNull(state);
        state = state.addWrite(put.table(), NewValue.transactionValue(put.cell(), put.value()));
    }

    @Override
    public <T> ListenableFuture<T> get(NewGetOperation<T> get) {
        checkNotNull(state);
        ResultBuilder<T> resultBuilder = get.newResultBuilder();
        return Futures.transform(iterators.takeWhile(
                scanner.scan(state, get.table(), get.attributes(), get.scanFilter()), value -> {
            resultBuilder.add(get.table(), value.cell(), value.data());
            return !resultBuilder.isDone();
        }), x -> resultBuilder.build(), state.getScheduler());
    }

    @Override
    public ListenableFuture<?> end(NewEndOperation end) {
        checkNotNull(state);
        switch (end) {
            case ABORT: return abort();
            case COMMIT: return commit();
        }
        throw Unreachable.unreachable(end);
    }

    private ListenableFuture<?> commit() {
        TransactionState snapshot = checkNotNull(state);
        state = null;
        if (!snapshot.hasWrites()) {
            return Futures.immediateFuture(null);
        }
        return FutureChain.start(snapshot.getScheduler(), snapshot)
                .then(s -> locks.lock(s.writeLockDescriptors()), TransactionState::addHeldLock)
                .defer(s -> locks.unlock(s.heldLocks()))
                .then(conflictChecker::checkForWriteWriteConflicts)
                .then(writer::write)
                .then($ -> timestamps.getCommitTimestamp(), TransactionState::withCommitTimestamp)
                // todo punch
                .then(conflictChecker::checkForReadWriteConflicts)
                // todo pre-commit-conditions if any
                .then(s -> locks.checkStillValid(s.heldLocks()))
                .then(writer::commit)
                .done();
    }

    private static ListenableFuture<?> writeToSweepQueue(TransactionState state) {

    }

    private ListenableFuture<?> abort() {
        state = null;
        return Futures.immediateFailedFuture(new RuntimeException("TODO define a real type here"));
    }
}
