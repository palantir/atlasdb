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

package com.palantir.atlasdb.v2.api.transaction.scanner;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.api.NewValue.KvsValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.exception.FailedConflictCheckingException;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

public final class ReadCommittedDataReader extends TransformingReader<KvsValue, CommittedValue> {
    private static final ListenableFuture<Optional<?>> EMPTY_FUTURE = Futures.immediateFuture(Optional.empty());
    private final Kvs kvs;
    private final NewLocks locks;

    public ReadCommittedDataReader(Kvs kvs, AsyncIterators iterators, NewLocks locks) {
        super(kvs, iterators);
        this.kvs = kvs;
        this.locks = locks;
    }

    @SuppressWarnings("unchecked")
    private static <T> ListenableFuture<Optional<T>> emptyOptionalFuture() {
        return (ListenableFuture<Optional<T>>) (ListenableFuture<?>) EMPTY_FUTURE;
    }

    @Override
    protected ListenableFuture<Iterator<CommittedValue>> transformPage(TransactionState state,
            ScanDefinition definition, List<KvsValue> page) {
        PostFilterOperation operation = new PostFilterOperation(state, definition.table());
        ListenableFuture<List<Optional<CommittedValue>>> filtered = Futures.allAsList(page.stream()
                .map(operation::startBySkippingOurOwnWrites)
                .collect(toList()));
        return Futures.transform(
                filtered, list -> list.stream().flatMap(Streams::stream).iterator(), MoreExecutors.directExecutor());
    }

    private final class PostFilterOperation {
        private final TransactionState state;
        private final Table table;

        private final Autobatcher<Long, Long> getCommitTimestampsFromKvs;
        private final Autobatcher<Long, OptionalLong> getCommitTimestampsWithoutAborting;
        private final Autobatcher<Long, ?> waitForTransactionLocks;
        private final Autobatcher<CellAtTimestampRequest, Optional<KvsValue>> getCellsAtTimestamps;

        private PostFilterOperation(TransactionState state, Table table) {
            this.state = state;
            this.table = table;
            getCommitTimestampsFromKvs = new Autobatcher<>(this::getCommitTimestampsFromKvs, state.scheduler());
            getCommitTimestampsWithoutAborting = new Autobatcher<>(
                    this::getCommitTimestampsWithoutAborting, state.scheduler());
            waitForTransactionLocks = new Autobatcher<>(this::waitForTransactionLocks, state.scheduler());
            getCellsAtTimestamps = new Autobatcher<>(this::getCellsAtTimestamps, state.scheduler());
        }

        private ListenableFuture<Optional<CommittedValue>> restartSearchAtLowerStartTimestamp(
                Cell cell, long newStartTimestamp) {
            return Futures.transformAsync(getCellAtTimestamp(cell, newStartTimestamp), maybeValue -> {
                if (!maybeValue.isPresent()) {
                    return emptyOptionalFuture();
                }
                return startBySkippingOurOwnWrites(maybeValue.get());
            }, MoreExecutors.directExecutor());
        }

        private ListenableFuture<Optional<CommittedValue>> startBySkippingOurOwnWrites(KvsValue value) {
            if (value.startTimestamp() == state.startTimestamp()) {
                return restartSearchAtLowerStartTimestamp(value.cell(), value.startTimestamp() - 1);
            }
            return nextCheckInTransactionCache(value);
        }

        // todo start ts vs read ts
        private ListenableFuture<Optional<CommittedValue>> nextCheckInTransactionCache(KvsValue value) {
            // first, if the txn is already committed and in the cache (expected to be true most of the time), we are done.
            OptionalLong maybeCachedCommitTs = kvs.getCachedCommitTimestamp(value.startTimestamp());
            if (maybeCachedCommitTs.isPresent()) {
                long commitTs = maybeCachedCommitTs.getAsLong();
                if (commitTs < state.readTimestamp()) {
                    return Futures.immediateFuture(Optional.of(value.toCommitted(commitTs)));
                } else {
                    // it was in the cache, and is not visible to our transaction. Time to restart!
                    return restartSearchAtLowerStartTimestamp(value.cell(), value.startTimestamp() - 1);
                }
            }
            // next, we try to acquire commit ts locks, if we can.
            boolean shouldBeCommittedByNow = value.startTimestamp() < state.immutableTimestamp();
            // this is a bit fiddly - it's here to avoid deadlocks as part of the transaction protocol
            boolean weAreCommittingAndTransactionStartedAfterUs =
                    state.commitTimestamp().isPresent() && state.startTimestamp() < value.startTimestamp();
            if (shouldBeCommittedByNow || weAreCommittingAndTransactionStartedAfterUs) {
                return skipToGettingCommitTimestampsFromKvs(value);
            }
            return proceedByWaitingForLocksOnStartTimestamp(value);
        }

        private ListenableFuture<Optional<CommittedValue>> proceedByWaitingForLocksOnStartTimestamp(KvsValue value) {
            return Futures.transformAsync(
                    waitForTransactionLock(value),
                    $ -> lookupCommitTimestampsInKvs(value),
                    MoreExecutors.directExecutor());
        }

        private ListenableFuture<Optional<CommittedValue>> skipToGettingCommitTimestampsFromKvs(KvsValue value) {
            return lookupCommitTimestampsInKvs(value);
        }

        private ListenableFuture<Optional<CommittedValue>> lookupCommitTimestampsInKvs(KvsValue value) {
            if (state.commitTimestamp().isPresent() && state.startTimestamp() < value.startTimestamp()) {
                ListenableFuture<OptionalLong> maybeStartTimestampFuture =
                        getCommitTimestampWithoutAborting(value.startTimestamp());
                return Futures.transformAsync(maybeStartTimestampFuture, maybeCommitTs -> {
                    if (!maybeCommitTs.isPresent()) {
                        // we are read write conflict checking, and we cannot validly pass this back.
                        throw new FailedConflictCheckingException();
                    }
                    return restartSearchIfCommittedDataOutOfRange(value, maybeCommitTs.getAsLong());
                }, MoreExecutors.directExecutor());
            }
            ListenableFuture<Long> startTimestampFuture = getCommitTimestampFromKvs(value.startTimestamp());
            return Futures.transformAsync(startTimestampFuture,
                    commitTs -> restartSearchIfCommittedDataOutOfRange(value, commitTs),
                    MoreExecutors.directExecutor());
        }

        private ListenableFuture<Optional<CommittedValue>> restartSearchIfCommittedDataOutOfRange(
                KvsValue value, long commitTs) {
            if (commitTs != TransactionConstants.FAILED_COMMIT_TS && commitTs < state.readTimestamp()) {
                return Futures.immediateFuture(Optional.of(value.toCommitted(commitTs)));
            }
            return restartSearchAtLowerStartTimestamp(value.cell(), value.startTimestamp() - 1);
        }

        private ListenableFuture<?> waitForTransactionLock(KvsValue kvsValue) {
            return waitForTransactionLocks.apply(kvsValue.startTimestamp());
        }

        private <T> ListenableFuture<Map<Long, T>> waitForTransactionLocks(Set<Long> timestamps) {
            return Futures.transform(locks.await(timestamps.stream().map(NewLockDescriptor::timestamp).collect(toSet())),
                    $ -> Maps.asMap(timestamps, $$ -> null), MoreExecutors.directExecutor());
        }

        private ListenableFuture<Long> getCommitTimestampFromKvs(long startTimestamp) {
            return getCommitTimestampsFromKvs.apply(startTimestamp);
        }

        private ListenableFuture<Map<Long, Long>> getCommitTimestampsFromKvs(Set<Long> timestamps) {
            return kvs.getCommitTimestamps(timestamps, ShouldAbortUncommittedWrites.YES);
        }

        private ListenableFuture<OptionalLong> getCommitTimestampWithoutAborting(long timestamp) {
            return getCommitTimestampsWithoutAborting.apply(timestamp);
        }

        private ListenableFuture<Map<Long, OptionalLong>> getCommitTimestampsWithoutAborting(Set<Long> timestamps) {
            return Futures.transform(
                    kvs.getCommitTimestamps(timestamps,
                            ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING),
                    map -> Maps.transformValues(Maps.asMap(timestamps, map::get), x -> toOptionalLong(x)),
                    MoreExecutors.directExecutor());
        }

        private ListenableFuture<Optional<KvsValue>> getCellAtTimestamp(Cell cell, long startTimestamp) {
            return getCellsAtTimestamps.apply(ImmutableCellAtTimestampRequest.of(cell, startTimestamp));
        }

        private ListenableFuture<Map<CellAtTimestampRequest, Optional<KvsValue>>> getCellsAtTimestamps(
                Set<CellAtTimestampRequest> requests) {
            // this will throw if the requests set has multiple timestamps for a cell.
            // This should never happen (or at least, the autobatchers should be used by one txn only).
            Map<Cell, Long> cellsAtTimestamps = requests.stream()
                    .collect(toMap(CellAtTimestampRequest::cell, CellAtTimestampRequest::timestamp));
            return Futures.transform(kvs.loadCellsAtTimestamps(table, cellsAtTimestamps),
                    cells -> Maps.transformValues(
                            Maps.asMap(requests, req -> cells.get(req.cell())),
                            Optional::ofNullable),
                    MoreExecutors.directExecutor());
        }

    }

    @Value.Immutable
    interface CellAtTimestampRequest {
        @Value.Parameter
        Cell cell();
        @Value.Parameter
        long timestamp();
    }

    private static final class Autobatcher<I, O> implements AsyncFunction<I, O>, Runnable {
        private final AsyncFunction<Set<I>, Map<I, O>> backingFunction;
        private final Executor executor;
        private final Map<I, SettableFuture<O>> futures = new LinkedHashMap<>();
        private boolean isScheduled = false;

        private Autobatcher(AsyncFunction<Set<I>, Map<I, O>> backingFunction,
                Executor executor) {
            this.backingFunction = backingFunction;
            this.executor = executor;
        }

        @Override
        public ListenableFuture<O> apply(I input) {
            ListenableFuture<O> future = futures.computeIfAbsent(input, $ -> SettableFuture.create());
            scheduleIfNecessary();
            return future;
        }

        private void scheduleIfNecessary() {
            if (isScheduled) {
                return;
            }
            isScheduled = true;
            executor.execute(this);
        }

        @Override
        public void run() {
            Map<I, SettableFuture<O>> futuresCopy = ImmutableMap.copyOf(futures);
            futures.clear();
            isScheduled = false;
            ListenableFuture<Map<I, O>> result;
            try {
                result = backingFunction.apply(futuresCopy.keySet());
            } catch (Exception e) {
                markAllAsFailed(futuresCopy, e);
                return;
            }
            futuresCopy.forEach((input, output) -> {
                output.setFuture(Futures.transform(
                        result,
                        map -> map.get(input),
                        MoreExecutors.directExecutor()));
            });
        }

        private void markAllAsFailed(Map<I, SettableFuture<O>> futures, Throwable thrown) {
            futures.values().forEach(future -> future.setException(thrown));
        }
    }

    private static OptionalLong toOptionalLong(Long value) {
        if (value == null) {
            return OptionalLong.empty();
        } else {
            return OptionalLong.of(value);
        }
    }
}
