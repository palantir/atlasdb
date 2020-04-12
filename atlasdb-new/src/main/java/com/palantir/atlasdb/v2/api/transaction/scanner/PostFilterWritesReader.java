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

import static com.palantir.logsafe.Preconditions.checkState;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.api.NewValue.KvsValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

import io.vavr.collection.LinkedHashMap;
import io.vavr.collection.Map;

public final class PostFilterWritesReader extends TransformingReader<KvsValue, CommittedValue> {
    private final Kvs delegate;
    private final NewLocks locks;
    private final ShouldAbortUncommittedWrites shouldAbortWrites;

    public PostFilterWritesReader(
            AsyncIterators iterators,
            Kvs delegate,
            NewLocks locks,
            ShouldAbortUncommittedWrites shouldAbortWrites) {
        super(delegate, iterators);
        this.delegate = delegate;
        this.locks = locks;
        this.shouldAbortWrites = shouldAbortWrites;
    }

    @Override
    protected ListenableFuture<Iterator<CommittedValue>> transformPage(
            TransactionState state, ScanDefinition definition, List<KvsValue> page) {
        return postFilterWrites(state.scheduler(), definition.table(), state.startTimestamp(), state.immutableTimestamp(), page);
    }

    @VisibleForTesting
    ListenableFuture<Iterator<CommittedValue>> postFilterWrites(
            Executor executor, Table table, long startTs, long immutableTimestamp, List<KvsValue> values) {
        Map<Cell, KvsValue> asCells = values.stream().collect(
                LinkedHashMap.collector(KvsValue::cell, Function.identity()));
        PostFilterState postFilterState = PostFilterState.initialize(table, startTs, immutableTimestamp, asCells);
        return FutureChain.start(executor, postFilterState)
                .whileTrue(PostFilterState::incomplete, this::postFilterRound)
                .alterState(state -> values.stream()
                        .flatMap(value -> state.committed().get(value.cell()).toJavaStream())
                        .iterator())
                .done();
    }

    private FutureChain<PostFilterState> postFilterRound(FutureChain<PostFilterState> chain) {
        return chain
                .alterState(this::markCachedCommitTsDataCommitted)
                .then(this::waitForOngoingTransactionsToCommit)
                .then(this::findCommitTimestamps, PostFilterState::processCommitTimestamps)
                .alterState(PostFilterState::reissueAllRemaining)
                .then(this::reissueReadsForDroppedCells, PostFilterState::processReissuedReads);
    }

    private PostFilterState markCachedCommitTsDataCommitted(PostFilterState state) {
        java.util.Map<Long, Long> commitTimestamps = new java.util.LinkedHashMap<>();
        for (KvsValue value : state.unknown().values()) {
            long startTimestamp = value.startTimestamp();
            OptionalLong maybeCommitTimestamp = delegate.getCachedCommitTimestamp(value.startTimestamp());
            if (maybeCommitTimestamp.isPresent()) {
                commitTimestamps.put(startTimestamp, maybeCommitTimestamp.getAsLong());
            }
        }
        return state.processCommitTimestamps(commitTimestamps);
    }

    private ListenableFuture<java.util.Map<Long, Long>> findCommitTimestamps(PostFilterState state) {
        Set<Long> unknownStartTimestamps = state.unknown().values().map(KvsValue::startTimestamp).toJavaSet();
        if (unknownStartTimestamps.isEmpty()) {
            return Futures.immediateFuture(Collections.emptyMap());
        }
        return delegate.getCommitTimestamps(unknownStartTimestamps, shouldAbortWrites);
    }

    private ListenableFuture<?> waitForOngoingTransactionsToCommit(PostFilterState postFilterState) {
        if (shouldAbortWrites == ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING) {
            return Futures.immediateFuture(null);
        }
        Set<Long> timestampsToCheck = postFilterState.unknown().values()
                .map(KvsValue::startTimestamp)
                .filter(ts -> ts > postFilterState.immutableTimestamp())
                .toJavaSet();
        if (timestampsToCheck.isEmpty()) {
            return Futures.immediateFuture(null);
        } else {
            return locks.await(NewLockDescriptor.fromTimestamps(timestampsToCheck));
        }
    }

    private ListenableFuture<List<KvsValue>> reissueReadsForDroppedCells(PostFilterState state) {
        if (state.toReissue().isEmpty()) {
            return Futures.immediateFuture(Collections.emptyList());
        }
        return Futures.transform(delegate.loadCellsAtTimestamps(state.table(), state.toReissue().toJavaMap()),
                timestamps -> ImmutableList.copyOf(timestamps.values()),
                MoreExecutors.directExecutor());
    }

    @Value.Immutable
    interface PostFilterState {
        Table table();
        long startTimestamp();
        long immutableTimestamp();
        Map<Cell, KvsValue> unknown();
        Map<Cell, CommittedValue> committed();

        Map<Cell, Long> toReissue();

        default boolean incomplete() {
            return !unknown().isEmpty() || !toReissue().isEmpty();
        }

        default PostFilterState processReissuedReads(List<KvsValue> values) {
            Map<Cell, KvsValue> unknown = unknown();
            for (KvsValue value : values) {
                checkState(toReissue().containsKey(value.cell()));
                unknown = unknown.put(value.cell(), value);
            }
            return withUnknown(unknown).withToReissue(LinkedHashMap.empty());
        }

        default PostFilterState processCommitTimestamps(java.util.Map<Long, Long> timestamps) {
            Map<Cell, KvsValue> newUnknown = LinkedHashMap.empty();
            Map<Cell, CommittedValue> committed = committed();
            Map<Cell, Long> toReissue = toReissue();
            for (KvsValue value : unknown().values()) {
                if (!timestamps.containsKey(value.startTimestamp())) {
                    newUnknown = newUnknown.put(value.cell(), value);
                } else {
                    long timestamp = timestamps.get(value.startTimestamp());
                    if (timestamp == TransactionConstants.FAILED_COMMIT_TS || timestamp > startTimestamp()) {
                        toReissue = toReissue.put(value.cell(), value.startTimestamp() - 1);
                    } else {
                        committed = committed.put(value.cell(), value.toCommitted(timestamp));
                    }
                }
            }
            return this.withUnknown(newUnknown)
                    .withCommitted(committed)
                    .withToReissue(toReissue);
        }

        default PostFilterState reissueAllRemaining() {
            Map<Cell, Long> toReissue = toReissue();
            for (KvsValue value : unknown().values()) {
                toReissue = toReissue.put(value.cell(), value.startTimestamp() - 1);
            }
            return withUnknown(LinkedHashMap.empty()).withToReissue(toReissue);
        }

        PostFilterState withUnknown(Map<Cell, KvsValue> unknown);
        PostFilterState withCommitted(Map<Cell, CommittedValue> committed);
        PostFilterState withToReissue(Map<Cell, Long> cells);

        static PostFilterState initialize(Table table, long startTimestamp, long immutableTimestamp, Map<Cell, KvsValue> values) {
            return ImmutablePostFilterState.builder()
                    .table(table)
                    .startTimestamp(startTimestamp) // todo this is a bit error prone
                    .immutableTimestamp(immutableTimestamp)
                    .unknown(values)
                    .committed(LinkedHashMap.empty())
                    .toReissue(LinkedHashMap.empty())
                    .build();
        }
    }
}
