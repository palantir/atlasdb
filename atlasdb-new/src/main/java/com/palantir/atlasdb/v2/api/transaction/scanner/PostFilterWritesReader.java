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

import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.v2.api.iterators.AsyncIterators;
import com.palantir.atlasdb.v2.api.api.NewIds.Cell;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.api.NewValue.KvsValue;
import com.palantir.atlasdb.v2.api.api.NewValue.NotYetCommittedValue;
import com.palantir.atlasdb.v2.api.api.ScanDefinition;
import com.palantir.atlasdb.v2.api.future.FutureChain;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.transaction.state.TransactionState;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;

public final class PostFilterWritesReader extends TransformingReader<KvsValue, CommittedValue> {
    private final Kvs delegate;
    private final NewLocks locks;
    private final ShouldAbortWrites shouldAbortWrites;

    public enum ShouldAbortWrites {NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING, YES }

    public PostFilterWritesReader(
            AsyncIterators iterators,
            Kvs delegate,
            NewLocks locks,
            ShouldAbortWrites shouldAbortWrites) {
        super(delegate, iterators);
        this.delegate = delegate;
        this.locks = locks;
        this.shouldAbortWrites = shouldAbortWrites;
    }

    @Override
    protected ListenableFuture<Iterator<CommittedValue>> transformPage(
            TransactionState state, ScanDefinition definition, List<KvsValue> page) {
        return postFilterWrites(state.scheduler(), definition.table(), state.immutableTimestamp(), page);
    }

    private ListenableFuture<Iterator<CommittedValue>> postFilterWrites(
            Executor executor, Table table, long immutableTimestamp, List<KvsValue> values) {
        Map<Cell, KvsValue> asCells = values.stream().collect(HashMap.collector(KvsValue::cell, Function.identity()));
        PostFilterState postFilterState = PostFilterState.initialize(table, immutableTimestamp, asCells);
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
                .then(this::findCommitTimestampsForUnknown, PostFilterState::processCommitTimestamps)
                .then(this::maybeAbortWrites, (state, $) -> state.withNotYetCommitted(HashMap.empty()))
                .then(this::reissueReadsForDroppedCells, PostFilterState::processReissuedReads);
    }

    private PostFilterState markCachedCommitTsDataCommitted(PostFilterState state) {
        java.util.Map<Long, Long> commitTimestamps = new java.util.HashMap<>();
        for (KvsValue value : state.unknown().values()) {
            long startTimestamp = value.startTimestamp();
            OptionalLong maybeCommitTimestamp = delegate.getCachedCommitTimestamp(value.startTimestamp());
            if (maybeCommitTimestamp.isPresent()) {
                commitTimestamps.put(startTimestamp, maybeCommitTimestamp.getAsLong());
            }
        }
        return state.processCommitTimestamps(commitTimestamps);
    }

    private ListenableFuture<java.util.Map<Long, Long>> findCommitTimestampsForUnknown(PostFilterState state) {
        Set<Long> unknownStartTimestamps = state.unknown().values().map(KvsValue::startTimestamp).toJavaSet();
        return delegate.getCommitTimestamps(unknownStartTimestamps);
    }

    private ListenableFuture<?> waitForOngoingTransactionsToCommit(PostFilterState postFilterState) {
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

    private ListenableFuture<?> maybeAbortWrites(PostFilterState state) {
        return Futures.immediateFuture(null);
    }

    private ListenableFuture<List<KvsValue>> reissueReadsForDroppedCells(PostFilterState state) {
        return Futures.transform(delegate.loadCellsAtTimestamps(state.table(), state.toReissue().toJavaMap()),
                timestamps -> ImmutableList.copyOf(timestamps.values()),
                MoreExecutors.directExecutor());
    }

    @Value.Immutable
    interface PostFilterState {
        Table table();
        long immutableTimestamp();
        Map<Cell, KvsValue> unknown();
        Map<Cell, CommittedValue> committed();

        // TODO properly handle this
        Map<Cell, NotYetCommittedValue> notYetCommitted();
        Map<Cell, Long> toReissue();

        default boolean incomplete() {
            return unknown().isEmpty() || notYetCommitted().isEmpty() || toReissue().isEmpty();
        }

        default PostFilterState processReissuedReads(List<KvsValue> values) {
            Map<Cell, KvsValue> unknown = unknown();
            Map<Cell, Long> toReissue = toReissue();
            for (KvsValue value : values) {
                checkState(toReissue.containsKey(value.cell()));
                toReissue = toReissue.remove(value.cell());
                unknown.put(value.cell(), value);
            }
            return withUnknown(unknown).withToReissue(toReissue);
        }

        default PostFilterState processCommitTimestamps(java.util.Map<Long, Long> timestamps) {
            Map<Cell, CommittedValue> committed = committed();
            Map<Cell, NotYetCommittedValue> notYetCommitted = notYetCommitted();
            Map<Cell, Long> toReissue = toReissue();
            unknown().forEach((cell, value) -> {
                if (!timestamps.containsKey(value.startTimestamp())) {
                    notYetCommitted.put(cell, value.toNotYetCommitted());
                } else {
                    long timestamp = timestamps.get(value.startTimestamp());
                    if (timestamp == TransactionConstants.FAILED_COMMIT_TS) {
                        toReissue.put(cell, value.startTimestamp()); // maybe -1
                    } else {
                        committed.put(cell, value.toCommitted(timestamp));
                    }
                }
            });
            return this.withUnknown(HashMap.empty())
                    .withCommitted(committed)
                    .withNotYetCommitted(notYetCommitted)
                    .withToReissue(toReissue);
        }

        PostFilterState withUnknown(Map<Cell, KvsValue> unknown);
        PostFilterState withCommitted(Map<Cell, CommittedValue> committed);
        PostFilterState withNotYetCommitted(Map<Cell, NotYetCommittedValue> notYetCommitted);
        PostFilterState withToReissue(Map<Cell, Long> cells);

        static PostFilterState initialize(Table table, long immutableTimestamp, Map<Cell, KvsValue> values) {
            return ImmutablePostFilterState.builder().table(table).immutableTimestamp(immutableTimestamp).unknown(values).build();
        }
    }
}
