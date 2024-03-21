/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.DelegatingTransactionKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.api.OrphanedSentinelDeleter;
import com.palantir.atlasdb.transaction.api.TransactionFailedRetriableException;
import com.palantir.atlasdb.transaction.api.TransactionReadSentinelBehavior;
import com.palantir.atlasdb.transaction.service.SimpleTransactionService;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Set;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class ReadSentinelHandlerTest {
    private static final Value SENTINEL_VALUE = Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP);
    private static final TableReference TEST_TABLE = TableReference.createFromFullyQualifiedName("a.test");
    private static final Cell CELL_1 = Cell.create(PtBytes.toBytes("row1"), PtBytes.toBytes("column1"));
    private static final Cell CELL_2 = Cell.create(PtBytes.toBytes("row2"), PtBytes.toBytes("column2"));
    private static final byte[] VALUE_BYTES = PtBytes.toBytes("value");

    private final KeyValueService keyValueService = new InMemoryKeyValueService(true);
    private final TransactionService transactionService = SimpleTransactionService.createV2(keyValueService);

    @Mock
    private OrphanedSentinelDeleter orphanedSentinelDeleter;

    private ReadSentinelHandler readSentinelHandler;

    @BeforeEach
    public void setUp() {
        readSentinelHandler = new ReadSentinelHandler(
                new DelegatingTransactionKeyValueService(keyValueService),
                transactionService,
                TransactionReadSentinelBehavior.THROW_EXCEPTION,
                orphanedSentinelDeleter);
    }

    @Test
    public void valueWithInvalidValueTimestampTreatedAsSentinel() {
        assertThat(ReadSentinelHandler.isSweepSentinel(SENTINEL_VALUE)).isTrue();
    }

    @Test
    public void valueWithValidTimestampNotTreatedAsSentinel() {
        assertThat(ReadSentinelHandler.isSweepSentinel(Value.create(VALUE_BYTES, 1L)))
                .isFalse();
        assertThat(ReadSentinelHandler.isSweepSentinel(Value.create(VALUE_BYTES, 3141592L)))
                .isFalse();
    }

    @Test
    public void atlasTombstoneNotTreatedAsSentinel() {
        assertThat(ReadSentinelHandler.isSweepSentinel(Value.create(PtBytes.EMPTY_BYTE_ARRAY, 1L)))
                .isFalse();
    }

    @Test
    public void deletesBareOrphanedSentinels() {
        ImmutableSet<Cell> cells = ImmutableSet.of(CELL_1, CELL_2);
        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, cells);
        Set<Cell> orphanedCells = readSentinelHandler.findAndMarkOrphanedSweepSentinelsForDeletion(
                TEST_TABLE, ImmutableMap.of(CELL_1, SENTINEL_VALUE, CELL_2, SENTINEL_VALUE));

        assertThat(orphanedCells).containsExactlyInAnyOrder(CELL_1, CELL_2);
        verify(orphanedSentinelDeleter).scheduleSentinelsForDeletion(TEST_TABLE, cells);
    }

    @ParameterizedTest
    @EnumSource(UncommittedValueType.class)
    public void deletesOrphanedSentinelUnderneathUncommittedTransactions(UncommittedValueType uncommittedValueType) {
        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(CELL_1));
        LongStream.of(10L, 20L, 30L, 40L, 50L)
                .forEach(startTimestamp -> writeUncommittedValueAtTimestamp(startTimestamp, uncommittedValueType));

        Set<Cell> orphanedSentinels = readSentinelHandler.findAndMarkOrphanedSweepSentinelsForDeletion(
                TEST_TABLE, ImmutableMap.of(CELL_1, SENTINEL_VALUE));

        assertThat(orphanedSentinels).containsExactlyInAnyOrder(CELL_1);
        verify(orphanedSentinelDeleter).scheduleSentinelsForDeletion(TEST_TABLE, ImmutableSet.of(CELL_1));
    }

    @Test
    public void doesNotDeleteSentinelWithCoveringCommittedValue() {
        keyValueService.addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(CELL_1));
        keyValueService.put(TEST_TABLE, ImmutableMap.of(CELL_1, VALUE_BYTES), 50L);
        transactionService.putUnlessExists(50L, 60L);

        Set<Cell> orphanedSentinels = readSentinelHandler.findAndMarkOrphanedSweepSentinelsForDeletion(
                TEST_TABLE, ImmutableMap.of(CELL_1, SENTINEL_VALUE));

        assertThat(orphanedSentinels).isEmpty();
    }

    @Test
    public void handleReadSentinelThrowsExceptionsIfConfigured() {
        assertThatThrownBy(() -> readSentinelHandler.handleReadSentinel())
                .isInstanceOf(TransactionFailedRetriableException.class)
                .hasMessageContaining("Tried to read a value that has been deleted.");
    }

    @Test
    public void handleReadSentinelDoesNothingIfConfigured() {
        ReadSentinelHandler ignoringReadSentinelHandler = new ReadSentinelHandler(
                new DelegatingTransactionKeyValueService(keyValueService),
                transactionService,
                TransactionReadSentinelBehavior.IGNORE,
                orphanedSentinelDeleter);

        assertThatCode(ignoringReadSentinelHandler::handleReadSentinel).doesNotThrowAnyException();
    }

    private void writeUncommittedValueAtTimestamp(long timestamp, UncommittedValueType uncommittedValueType) {
        keyValueService.put(TEST_TABLE, ImmutableMap.of(CELL_1, VALUE_BYTES), timestamp);
        switch (uncommittedValueType) {
            case EXPLICIT_ABORT:
                transactionService.putUnlessExists(timestamp, TransactionConstants.FAILED_COMMIT_TS);
                break;
            case NO_ENTRY_IN_TRANSACTIONS_TABLE:
                // Empty block is intentional: there's nothing more to do in this case.
                break;
            default:
                throw new SafeIllegalStateException(
                        "Unexpected uncommitted value type", SafeArg.of("uncommittedValueType", uncommittedValueType));
        }
    }

    private enum UncommittedValueType {
        EXPLICIT_ABORT,
        NO_ENTRY_IN_TRANSACTIONS_TABLE;
    }
}
