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

package com.palantir.atlasdb.v2.api.kvs;

import java.util.List;

import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.v2.api.api.AsyncIterator;
import com.palantir.atlasdb.v2.api.api.ConflictChecker;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.exception.FailedConflictCheckingException;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.transaction.scanner.Reader;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderChain;
import com.palantir.atlasdb.v2.api.transaction.scanner.ReaderFactory;
import com.palantir.atlasdb.v2.api.transaction.scanner.ShouldAbortUncommittedWrites;
import com.palantir.atlasdb.v2.api.transaction.state.TableReads;
import com.palantir.atlasdb.v2.api.transaction.state.TableWrites;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public class DefaultConflictChecker implements ConflictChecker {
    private final AsyncIterators iterators;
    private final Reader<CommittedValue> readLatestCommittedTimestamps;
    private final Reader<NewValue> readAtCommitTimestamp;

    public DefaultConflictChecker(AsyncIterators iterators, ReaderFactory readerFactory) {
        this.iterators = iterators;
        this.readLatestCommittedTimestamps = ReaderChain.create(readerFactory.kvs())
                .then(readerFactory.postFilterWrites(ShouldAbortUncommittedWrites.YES))
                .then(readerFactory.readAtVeryLatestTimestamp())
                .then(readerFactory.stopAfterMarker())
                .then(readerFactory.orderValidating())
                .build();
        this.readAtCommitTimestamp = ReaderChain.create(readerFactory.kvs())
                // it's fiddly as to why this is safe... but it is.
                .then(readerFactory.postFilterWrites(ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING))
                .then(readerFactory.readAtCommitTimestamp())
                .then(readerFactory.mergeInTransactionWrites())
                .then(readerFactory.stopAfterMarker())
                .then(readerFactory.orderValidating())
                .build();
    }

    @Override
    public ListenableFuture<?> checkForWriteWriteConflicts(TransactionState state) {
        ListenableFuture<List<Object>> result = Futures.allAsList(Iterables.transform(state.writes(),
                writes -> checkForWriteWriteConflicts(state, writes)));
        if (state.debugging()) {
            Futures.addCallback(result, new FutureCallback<Object>() {
                @Override
                public void onSuccess(@NullableDecl Object result) {
                    System.out.println("Here");
                }

                @Override
                public void onFailure(Throwable t) {
                    System.out.println("Here");
                }
            });
        }
        return result;
    }

    private ListenableFuture<?> checkForWriteWriteConflicts(TransactionState state, TableWrites writes) {
        ScanDefinition scan = writes.toConflictCheckingScan();
        AsyncIterator<CommittedValue> executed = readLatestCommittedTimestamps.scan(state, scan);
        if (state.debugging()) {
            System.out.println("Here");
        }
        return iterators.forEach(executed, element -> {
            if (element.commitTimestamp() >= state.startTimestamp()) {
                throw new FailedConflictCheckingException();
            }
        });
    }

    @Override
    public ListenableFuture<?> checkForReadWriteConflicts(TransactionState state) {
        return Futures.allAsList(Iterables.transform(state.reads(), reads -> checkForReadWriteConflicts(state, reads)));
    }

    private ListenableFuture<?> checkForReadWriteConflicts(TransactionState state, TableReads reads) {
        Iterable<ScanDefinition> scans = reads.toConflictCheckingScans();
        return Futures.allAsList(Iterables.transform(scans, scan -> {
            Table table = scan.table();
            TableWrites writes = state.writes().get(table).orElse(new TableWrites.Builder().table(table).build());
            AsyncIterator<NewValue> executed = readAtCommitTimestamp.scan(state, scan);
            return iterators.forEach(executed, element -> {
                // todo: I _think_ that we're guaranteed to see at least Atlas tombstones for values due to immutable ts properties.
                if (!writes.containsCell(element.cell()) && !reads.get(element.cell()).equals(element.maybeData())) {
                    throw new FailedConflictCheckingException();
                }
            });
        }));
    }
}
