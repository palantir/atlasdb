/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.palantir.logsafe.testing.Assertions.assertThat;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.GuardedValue;
import com.palantir.atlasdb.keyvalue.api.ImmutableGuardedValue;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.ImmutableLockWatch;
import com.palantir.lock.v2.LockWatch;

public class TransactionLockWatchingCacheViewTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final byte[] VALUE = PtBytes.toBytes("value");
    private static final byte[] VALUE2 = PtBytes.toBytes("value_2");


    private final LockWatchingCache cache = mock(LockWatchingCache.class);
    private final Map<Cell, GuardedValue> cachedValues = ImmutableMap.of(
            cell(1), ImmutableGuardedValue.of(VALUE, 1L),
            cell(2), ImmutableGuardedValue.of(VALUE, 100L),
            cell(3), ImmutableGuardedValue.of(VALUE, 1000L),
            cell(4), ImmutableGuardedValue.of(VALUE2, 100L));

    @Before
    public void setupMock() {
        when(cache.getCached(any(TableReference.class), anySet()))
                .thenAnswer(args -> {
                    Set<Cell> cells = (Set<Cell>) args.getArguments()[1];
                    return cells.stream()
                            .filter(cachedValues::containsKey)
                            .collect(Collectors.toMap(x -> x, cachedValues::get));
                });
    }

    private TransactionLockWatchingCacheView view;

    @Test
    public void nothingIsReturnedWhenViewHasNoWatches() {
        setupView(ImmutableMap.of());
        assertThat(view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2), cell(3)))).isEmpty();
    }

    @Test
    public void nothingIsReturnedWhenNothingIsRequested() {
        setupView(ImmutableMap.of(1, committed(1L)));
        assertThat(view.readCached(TABLE, ImmutableSet.of())).isEmpty();
    }

    @Test
    public void returnsCachedValuesFromCommittedTransactionsWithMatchingTimestamps() {
        setupView(ImmutableMap.of(1, committed(1L), 2, committed(100L)));
        Map<Cell, byte[]> result = view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2)));

        assertThat(result.size()).isEqualTo(2);
        assertThat(result).containsAllEntriesOf(ImmutableMap.of(cell(1), VALUE, cell(2), VALUE));
    }

    @Test
    public void returnsOnlyRequestedCachedValues() {
        setupView(ImmutableMap.of(1, committed(1L), 2, committed(100L)));
        Map<Cell, byte[]> result = view.readCached(TABLE, ImmutableSet.of(cell(1)));

        assertThat(result.size()).isEqualTo(1);
        assertThat(result).containsAllEntriesOf(ImmutableMap.of(cell(1), VALUE));
    }

    @Test
    public void doesNotReturnValuesThatAreNotCached() {
        setupView(ImmutableMap.of(1, committed(1L), 5, committed(42L)));
        Map<Cell, byte[]> result = view.readCached(TABLE, ImmutableSet.of(cell(1), cell(5)));

        assertThat(result.size()).isEqualTo(1);
        assertThat(result).containsAllEntriesOf(ImmutableMap.of(cell(1), VALUE));
    }

    @Test
    public void doesNotReturnCachedValuesWithNonMatchingTimestamp() {
        setupView(ImmutableMap.of(1, committed(1L), 2, committed(42L)));
        Map<Cell, byte[]> result = view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2)));

        assertThat(result.size()).isEqualTo(1);
        assertThat(result).containsAllEntriesOf(ImmutableMap.of(cell(1), VALUE));
    }

    @Test
    public void doesNotReturnCachedValuesFromUncommittedTransactions() {
        setupView(ImmutableMap.of(1, committed(1L), 2, uncommitted(100L), 4, committed(100L)));
        Map<Cell, byte[]> result = view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2), cell(4)));

        assertThat(result.size()).isEqualTo(2);
        assertThat(result).containsAllEntriesOf(ImmutableMap.of(cell(1), VALUE, cell(4), VALUE2));
    }

    private void setupView(Map<Integer, LockWatch> watches) {
        view = new TransactionLockWatchingCacheView(
                5000L,
                KeyedStream.stream(watches)
                        .mapKeys(TransactionLockWatchingCacheViewTest::lockDescriptor)
                        .collectToMap(),
                mock(KeyValueService.class),
                cache);
    }

    private static Cell cell(int num) {
        return Cell.create(PtBytes.toBytes(num), PtBytes.toBytes(num));
    }

    private static LockDescriptor lockDescriptor(int num) {
        return AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), cell(num).getRowName());
    }

    private static LockWatch committed(long timestamp) {
        return ImmutableLockWatch.of(timestamp, true);
    }

    private static LockWatch uncommitted(long timestamp) {
        return ImmutableLockWatch.of(timestamp, false);
    }
}
