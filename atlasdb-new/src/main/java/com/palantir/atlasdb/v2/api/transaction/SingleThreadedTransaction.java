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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.NewEndOperation;
import com.palantir.atlasdb.v2.api.api.NewGetOperation;
import com.palantir.atlasdb.v2.api.api.NewGetOperation.ResultBuilder;
import com.palantir.atlasdb.v2.api.api.NewPutOperation;
import com.palantir.atlasdb.v2.api.api.NewTransaction;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.api.ConflictChecker;
import com.palantir.atlasdb.v2.api.api.Writer;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.api.Timestamps;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReadReportingReader.RecordingNewValue;
import com.palantir.atlasdb.v2.api.transaction.scanner.Reader;
import com.palantir.atlasdb.v2.api.transaction.state.StateHolder;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;
import com.palantir.atlasdb.v2.api.util.Unreachable;

public class SingleThreadedTransaction implements NewTransaction {
    private final Reader<RecordingNewValue> reader;
    private final Writer writer;
    private final NewLocks locks;
    private final ConflictChecker conflictChecker;
    private final AsyncIterators iterators;
    private final Timestamps timestamps;
    private final StateHolder stateHolder;

    public SingleThreadedTransaction(
            Reader<RecordingNewValue> reader,
            Writer writer,
            Timestamps timestamps,
            NewLocks locks,
            ConflictChecker conflictChecker,
            AsyncIterators iterators,
            TransactionState initialState) {
        this.reader = reader;
        this.writer = writer;
        this.locks = locks;
        this.conflictChecker = conflictChecker;
        this.iterators = iterators;
        this.timestamps = timestamps;
        this.stateHolder = new StateHolder(initialState);
    }

    @Override
    public void setIsDebugging() {
        stateHolder.mutate(t -> t.debugging(true));
    }

    @Override
    public void put(NewPutOperation put) {
        stateHolder.mutate(state -> state.writesBuilder().mutateWrites(put.table(),
                t -> t.put(NewValue.transactionValue(put.cell(), put.value()))));
    }

    @Override
    public <T> ListenableFuture<T> get(NewGetOperation<T> get) {
        ResultBuilder<T> resultBuilder = get.newResultBuilder();
        TransactionState state = stateHolder.get();
        ScanDefinition definition = ScanDefinition.of(get.table(), get.scanFilter(), get.attributes());

        return Futures.transform(iterators.takeWhile(
                stateHolder.iterate(reader.scan(state, definition),
                        s -> s.readsBuilder().mutateReads(definition.table(), reads -> reads.reachedEnd(definition))),
                value -> {
                    resultBuilder.add(get.table(), value.cell(), value.maybeData().get());
                    return !resultBuilder.isDone();
                }), x -> resultBuilder.build(), state.scheduler());
    }

    @Override
    public ListenableFuture<?> end(NewEndOperation end) {
        switch (end) {
            case ABORT: return abort();
            case COMMIT: return commit();
        }
        throw Unreachable.unreachable(end);
    }

    // currently assume that someone else is unlocking anything the immutable ts lock
    private ListenableFuture<?> commit() {
        TransactionState state = stateHolder.invalidateAndGet();
        if (state.writes().isEmpty()) {
            return Futures.immediateFuture(null);
        }
        // this is eager some way... maybe it should be lazy?
        return FutureChain.start(state.scheduler(), state)
                .then(s -> locks.lock(s.writeLockDescriptors()),
                        (transactionState, token) -> transactionState.toBuilder().addHeldLocks(token).build())
                .defer(s -> locks.unlock(s.heldLocks()))
                .then(conflictChecker::checkForWriteWriteConflicts)
                .then(writer::write)
                .then($ -> timestamps.getCommitTimestamp(), (s, ts) -> s.toBuilder().commitTimestamp(ts).build())
                // todo punch
                .then(conflictChecker::checkForReadWriteConflicts)
                // todo pre-commit-conditions if any
                .then(s -> locks.checkStillValid(s.heldLocks()))
                .then(writer::commit)
                .maskedDone();
    }

    private ListenableFuture<?> abort() {
        if (!stateHolder.isInvalid()) {
            stateHolder.invalidateAndGet();
        }
        return Futures.immediateFailedFuture(new RuntimeException("TODO define a real type here"));
    }
}
