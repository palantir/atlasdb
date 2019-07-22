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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.timelock.watch.ExplicitLockPredicate;
import com.palantir.atlasdb.timelock.watch.ImmutableRegisterWatchResponse;
import com.palantir.atlasdb.timelock.watch.ImmutableWatchStateResponse;
import com.palantir.atlasdb.timelock.watch.LockWatchRpcClient;
import com.palantir.atlasdb.timelock.watch.WatchIdentifier;
import com.palantir.atlasdb.timelock.watch.WatchIndexState;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.lock.AtlasRowLockDescriptor;

// TODO (jkong): Uninterruptibles! Probably better to implement a more deterministic cache flushing
public class LockWatchClientImplTest {
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("a.b");

    private static final byte[] BYTES = { 1, 2, 3 };
    private static final RowReference ROW_REFERENCE = ImmutableRowReference.builder()
            .tableReference(TEST_TABLE)
            .row(BYTES)
            .build();

    private final KeyValueService kvs = spy(new InMemoryKeyValueService(true));
    private final TransactionService transactionService = TransactionServices.createV1TransactionService(kvs);
    private final ConflictDetectionManager detectionManager = ConflictDetectionManagers.create(kvs);
    private final LockWatchRpcClient rpcClient = mock(LockWatchRpcClient.class);
    private final LockWatchClientImpl lockWatchClient = new LockWatchClientImpl(
            kvs, transactionService, rpcClient, detectionManager);

    @Rule
    public final FlakeRetryingRule retryingRule = new FlakeRetryingRule();

    @Before
    public void makeTestTable() {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.truncateTable(TEST_TABLE);
    }

    @After
    public void mockReset() {
        reset(kvs);
    }

    @Test
    public void passthrough() {
        when(rpcClient.registerOrGetStates(any())).thenReturn(ImmutableWatchStateResponse.builder().build());

        kvs.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(BYTES, PtBytes.toBytes("q")), PtBytes.toBytes("v")), 42);

        Map<Cell, Value> v1 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 14);
        Map<Cell, Value> v2 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 1234);

        assertThat(v1).isEmpty();
        assertThat(v2).isNotEmpty();
    }

    @Test
    public void irrelevant() {
        when(rpcClient.registerOrGetStates(any())).thenReturn(ImmutableWatchStateResponse.builder()
                .addRegisterResponses(ImmutableRegisterWatchResponse.builder()
                        .identifier(WatchIdentifier.of("o"))
                        .predicate(ExplicitLockPredicate.of(AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(),
                                BYTES)))
                        .indexState(WatchIndexState.of(1, 2)).build())
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(1, 2))
                .build());

        kvs.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(BYTES, PtBytes.toBytes("q")), PtBytes.toBytes("v")), 42);

        Map<Cell, Value> v1 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 99);
        Map<Cell, Value> v2 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 1234);

        assertThat(v1).isNotEmpty();
        assertThat(v2).isNotEmpty();
    }

    @Test
    public void watch() {
        when(rpcClient.registerOrGetStates(any())).thenReturn(ImmutableWatchStateResponse.builder()
                .addRegisterResponses(ImmutableRegisterWatchResponse.builder()
                        .identifier(WatchIdentifier.of("o"))
                        .predicate(ExplicitLockPredicate.of(AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(),
                                BYTES)))
                        .indexState(WatchIndexState.of(1, 2)).build())
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(1, 2))
                .build());

        lockWatchClient.enableWatchForRow(ROW_REFERENCE);

        kvs.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(BYTES, PtBytes.toBytes("q")), PtBytes.toBytes("v")), 42);

        Map<Cell, Value> v1 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 99);
        assertThat(v1).isNotEmpty();

        // flush that cache
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        for (int i = 0; i < 1000; i++) {
            Map<Cell, Value> v2 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(),
                    ThreadLocalRandom.current().nextInt(43, 135295));
            assertThat(v2).isNotEmpty();
        }

        // A bit of leeway in case the cache is slow
        verify(kvs, atMost(5)).getRows(eq(TEST_TABLE), any(), any(), anyLong());
    }

    @Test
    public void recoverFromBreak() {
        when(rpcClient.registerOrGetStates(any())).thenReturn(ImmutableWatchStateResponse.builder()
                .addRegisterResponses(ImmutableRegisterWatchResponse.builder()
                        .identifier(WatchIdentifier.of("o"))
                        .predicate(ExplicitLockPredicate.of(AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(),
                                BYTES)))
                        .indexState(WatchIndexState.of(1, 2)).build())
                .build())
                .thenReturn(ImmutableWatchStateResponse.builder()
                        .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(1, 2))
                        .build())
                .thenReturn(ImmutableWatchStateResponse.builder()
                        .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(1, 2))
                        .build())
                .thenReturn(ImmutableWatchStateResponse.builder()
                        .build())
                .thenReturn(ImmutableWatchStateResponse.builder()
                .addRegisterResponses(ImmutableRegisterWatchResponse.builder()
                        .identifier(WatchIdentifier.of("p"))
                        .predicate(ExplicitLockPredicate.of(AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(),
                                BYTES)))
                        .indexState(WatchIndexState.of(1, 2)).build())
                .build())
                .thenReturn(ImmutableWatchStateResponse.builder()
                        .putStateResponses(WatchIdentifier.of("p"), WatchIndexState.of(1, 2))
                        .build());

        lockWatchClient.enableWatchForRow(ROW_REFERENCE);

        kvs.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(BYTES, PtBytes.toBytes("q")), PtBytes.toBytes("v")), 42);

        Map<Cell, Value> v1 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 99);
        assertThat(v1).isNotEmpty();

        // flush that cache
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        for (int i = 0; i < 1000; i++) {
            Map<Cell, Value> v2 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(),
                    ThreadLocalRandom.current().nextInt(43, 135295));
            assertThat(v2).isNotEmpty();
        }

        // A bit of leeway in case the cache is slow
        verify(kvs, atMost(6)).getRows(eq(TEST_TABLE), any(), any(), anyLong());
    }

    @Test
    public void dontCacheTooMuch() {
        when(rpcClient.registerOrGetStates(any())).thenReturn(ImmutableWatchStateResponse.builder()
                .addRegisterResponses(ImmutableRegisterWatchResponse.builder()
                        .identifier(WatchIdentifier.of("o"))
                        .predicate(ExplicitLockPredicate.of(AtlasRowLockDescriptor.of(TEST_TABLE.getQualifiedName(),
                                BYTES)))
                        .indexState(WatchIndexState.of(1, 2)).build())
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(1, 2))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(3, 4))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(5, 6))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(7, 8))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(9, 10))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(11, 12))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(13, 14))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(15, 16))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(17, 18))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(19, 20))
                .build()).thenReturn(ImmutableWatchStateResponse.builder()
                .putStateResponses(WatchIdentifier.of("o"), WatchIndexState.of(21, 22))
                .build());

        lockWatchClient.enableWatchForRow(ROW_REFERENCE);

        kvs.put(TEST_TABLE, ImmutableMap.of(
                Cell.create(BYTES, PtBytes.toBytes("q")), PtBytes.toBytes("v")), 42);

        Map<Cell, Value> v1 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(), 99);
        assertThat(v1).isNotEmpty();

        // flush that cache
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

        for (int i = 0; i < 5000; i++) {
            Map<Cell, Value> v2 = lockWatchClient.getRows(TEST_TABLE, ImmutableList.of(BYTES), ColumnSelection.all(),
                    ThreadLocalRandom.current().nextInt(43, 135295));
            assertThat(v2).isNotEmpty();
        }

        // A bit of leeway in case the cache is slow
        verify(kvs, atLeast(12)).getRows(eq(TEST_TABLE), any(), any(), anyLong());
        verify(kvs, atMost(2500)).getRows(eq(TEST_TABLE), any(), any(), anyLong());
    }
}
