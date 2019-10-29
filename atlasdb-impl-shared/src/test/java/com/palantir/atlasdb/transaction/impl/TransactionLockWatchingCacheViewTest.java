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
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.GuardedValue;
import com.palantir.atlasdb.keyvalue.api.ImmutableGuardedValue;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.v2.LockWatch;

public class TransactionLockWatchingCacheViewTest {
    private static final TableReference TABLE = TableReference.createFromFullyQualifiedName("test.table");
    private static final byte[] VALUE = PtBytes.toBytes("value");

    private final LockWatchingCache cache = mock(LockWatchingCache.class);
    private final Map<Cell, GuardedValue> cachedValues = Maps.newHashMap();

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
        cachedValues.putAll(ImmutableMap.of(
                cell(1), ImmutableGuardedValue.of(VALUE, 1L),
                cell(2), ImmutableGuardedValue.of(VALUE, 100L),
                cell(3), ImmutableGuardedValue.of(VALUE, 1000L)
        ));
        view = new TransactionLockWatchingCacheView(100L, ImmutableMap.of(), mock(KeyValueService.class), cache);
        assertThat(view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2), cell(3)))).isEmpty();
    }

    @Test
    public void () {
        cachedValues.putAll(ImmutableMap.of(
                cell(1), ImmutableGuardedValue.of(VALUE, 1L),
                cell(2), ImmutableGuardedValue.of(VALUE, 100L),
                cell(3), ImmutableGuardedValue.of(VALUE, 1000L)
        ));
        view = new TransactionLockWatchingCacheView(100L, ImmutableMap.of(lockDescriptor(1), 1L, lockDescriptor(2), 100L), mock(KeyValueService.class), cache);
        assertThat(view.readCached(TABLE, ImmutableSet.of(cell(1), cell(2), cell(3)))).isEmpty();
    }

    private static Cell cell(int a) {
        return Cell.create(PtBytes.toBytes(a), PtBytes.toBytes(a));
    }

    private static LockDescriptor lockDescriptor(int a) {
        return AtlasRowLockDescriptor.of(TABLE.getQualifiedName(), cell(a).getRowName());
    }
}
