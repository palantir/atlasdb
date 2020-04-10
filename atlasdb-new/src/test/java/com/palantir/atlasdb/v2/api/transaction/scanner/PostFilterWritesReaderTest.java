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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.v2.api.api.Kvs;
import com.palantir.atlasdb.v2.api.api.NewIds;
import com.palantir.atlasdb.v2.api.api.NewIds.Table;
import com.palantir.atlasdb.v2.api.api.NewLockDescriptor;
import com.palantir.atlasdb.v2.api.api.NewLocks;
import com.palantir.atlasdb.v2.api.api.NewValue;
import com.palantir.atlasdb.v2.api.api.NewValue.CommittedValue;
import com.palantir.atlasdb.v2.api.api.NewValue.KvsValue;

@RunWith(MockitoJUnitRunner.class)
public class PostFilterWritesReaderTest {
    private static final Table TABLE = NewIds.table("table");
    private static final NewIds.Cell CELL = NewIds.cell(NewIds.row(new byte[1]), NewIds.column(new byte[2]));
    private static final long IMMUTABLE_TS = 12345;
    private static final long START_TS = 13000;
    @Mock private Kvs kvs;
    @Mock private NewLocks locks;

    @Before
    public void before() {
        when(kvs.getCachedCommitTimestamp(anyLong())).thenReturn(OptionalLong.empty());
        when(locks.await(any())).thenReturn(Futures.immediateFuture(null));
        when(kvs.getCommitTimestamps(any(), any()))
                .thenAnswer(inv -> {
                    Set<Long> args = inv.getArgument(0);
                    return Futures.immediateFuture(Maps.toMap(args, PostFilterWritesReaderTest::commitTs));
                });
        when(kvs.loadCellsAtTimestamps(eq(TABLE), any()))
                .thenReturn(Futures.immediateFuture(Collections.emptyMap()));
    }

    private static long commitTs(long startTs) {
        return startTs + 1;
    }

    private List<CommittedValue> postFilterWrites(
            List<KvsValue> values,
            ShouldAbortUncommittedWrites shouldAbortWrites) {
        PostFilterWritesReader reader = new PostFilterWritesReader(null, kvs, locks, shouldAbortWrites);
        ListenableFuture<Iterator<CommittedValue>> page = reader.postFilterWrites(
                MoreExecutors.directExecutor(), TABLE, START_TS, IMMUTABLE_TS, values);
        return ImmutableList.copyOf(Futures.getUnchecked(page));
    }

    private List<CommittedValue> postFilterWrites(KvsValue... values) {
        return postFilterWrites(Arrays.asList(values), ShouldAbortUncommittedWrites.YES);
    }

    private List<CommittedValue> conflictCheckingPostFilterWrites(KvsValue... values) {
        return postFilterWrites(Arrays.asList(values), ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING);
    }

    private static KvsValue write(long ts) {
        return NewValue.kvsValue(CELL, ts, Optional.empty());
    }

    @Test
    public void returnsImmediatelyIfCommitTimestampsInCache() {
        when(kvs.getCachedCommitTimestamp(1)).thenReturn(OptionalLong.of(2));
        KvsValue cell = write(1);
        assertThat(postFilterWrites(cell)).containsExactly(cell.toCommitted(2));
        verify(kvs).getCachedCommitTimestamp(1);
        verifyNoMoreInteractions(kvs);
    }

    @Test
    public void waitsForTransactionsAfterImmutableTsToBeCommitted() {
        postFilterWrites(write(IMMUTABLE_TS - 1));
        verifyNoInteractions(locks);
        postFilterWrites(write(IMMUTABLE_TS + 1));
        verify(locks).await(ImmutableSet.of(NewLockDescriptor.timestamp(IMMUTABLE_TS + 1)));
    }

    @Test
    public void doesNotWaitForTransactionsAfterImmutableTsToBeCommittedIfConflictChecking() {
        conflictCheckingPostFilterWrites(write(IMMUTABLE_TS + 1));
        verifyNoInteractions(locks);
    }

    @Test
    public void returnsIfCouldLoadCommitTimestampsFromDb() {
        long ts = 1;
        postFilterWrites(write(ts));
        verify(kvs).getCommitTimestamps(ImmutableSet.of(ts), ShouldAbortUncommittedWrites.YES);
    }

    @Test
    public void abortsUncommittedWritesIfNotConflictChecking() {
        long ts = 1;
        postFilterWrites(write(ts));
        verify(kvs).getCommitTimestamps(ImmutableSet.of(ts), ShouldAbortUncommittedWrites.YES);
    }

    @Test
    public void doesNotAbortUncommittedWritesIfConflictChecking() {
        long ts = 1;
        conflictCheckingPostFilterWrites(write(ts));
        verify(kvs).getCommitTimestamps(ImmutableSet.of(ts),
                ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING);
    }

    @Test
    public void repeatsIfWritesWereAborted_loadingAtEarlierTimestamp() {
        long ts = 100;
        long actualTs = 80;
        when(kvs.getCommitTimestamps(ImmutableSet.of(ts), ShouldAbortUncommittedWrites.YES))
                .thenReturn(Futures.immediateFuture(Collections.singletonMap(ts, TransactionConstants.FAILED_COMMIT_TS)));
        when(kvs.loadCellsAtTimestamps(TABLE, Collections.singletonMap(CELL, ts - 1)))
                .thenReturn(Futures.immediateFuture(Collections.singletonMap(CELL, write(actualTs))));
        assertThat(postFilterWrites(write(ts))).containsExactly(write(actualTs).toCommitted(commitTs(actualTs)));
    }

    @Test
    public void repeatsIfWritesCommittedAfterStartTimestamp() {
        long ts = 100;
        long actualTs = 80;
        when(kvs.getCommitTimestamps(ImmutableSet.of(ts), ShouldAbortUncommittedWrites.YES))
                .thenReturn(Futures.immediateFuture(Collections.singletonMap(ts, START_TS + 1)));
        when(kvs.loadCellsAtTimestamps(TABLE, Collections.singletonMap(CELL, ts - 1)))
                .thenReturn(Futures.immediateFuture(Collections.singletonMap(CELL, write(actualTs))));
        assertThat(postFilterWrites(write(ts))).containsExactly(write(actualTs).toCommitted(commitTs(actualTs)));
    }

    @Test
    public void repeatsIfWritesInProgress_ifWeAreConflictChecking() {
        long ts = 100;
        long actualTs = 80;
        when(kvs.getCommitTimestamps(ImmutableSet.of(ts), ShouldAbortUncommittedWrites.NO_WE_ARE_READ_WRITE_CONFLICT_CHECKING))
                .thenReturn(Futures.immediateFuture(Collections.emptyMap()));
        when(kvs.loadCellsAtTimestamps(TABLE, Collections.singletonMap(CELL, ts - 1)))
                .thenReturn(Futures.immediateFuture(Collections.singletonMap(CELL, write(actualTs))));
        assertThat(conflictCheckingPostFilterWrites(write(ts)))
                .containsExactly(write(actualTs).toCommitted(commitTs(actualTs)));
    }
}
