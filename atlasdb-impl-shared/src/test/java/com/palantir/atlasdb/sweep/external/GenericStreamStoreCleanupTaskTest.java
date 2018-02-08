/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.external;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class GenericStreamStoreCleanupTaskTest {
    private static final byte[] ROW_1 = { 1 };
    private static final byte[] ROW_2 = { 12 };
    private static final byte[] COLUMN = { 2 };
    private static final Cell CELL_1 = Cell.create(ROW_1, COLUMN);
    private static final Cell CELL_2 = Cell.create(ROW_2, COLUMN);
    private static final GenericStreamIdentifier IDENTIFIER_1 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_1);
    private static final GenericStreamIdentifier IDENTIFIER_2 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_2);

    private final Transaction tx = mock(Transaction.class);

    private final GenericStreamStoreRowDecoder rowDecoder = mock(GenericStreamStoreRowDecoder.class);
    private final GenericStreamDeletionFilter deletionFilter = mock(GenericStreamDeletionFilter.class);
    private final SchemalessStreamStoreDeleter deleter = mock(SchemalessStreamStoreDeleter.class);
    private final GenericStreamStoreCleanupTask cleanupTask =
            new GenericStreamStoreCleanupTask(rowDecoder, deletionFilter, deleter);

    @Before
    public void setUp() {
        when(rowDecoder.decodeIndexOrMetadataTableRow(ROW_1)).thenReturn(IDENTIFIER_1);
        when(rowDecoder.decodeIndexOrMetadataTableRow(ROW_2)).thenReturn(IDENTIFIER_2);
    }

    @Test
    public void deletesStreamsThatFilterReturns() {
        when(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1)))
                .thenReturn(ImmutableSet.of(IDENTIFIER_1));

        cleanupTask.cellsCleanedUp(tx, ImmutableSet.of(CELL_1));
        verify(deleter).deleteStreams(tx, ImmutableSet.of(IDENTIFIER_1));
    }

    @Test
    public void doesNotDeleteStreamsThatFilterDoesNotReturn() {
        when(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1)))
                .thenReturn(ImmutableSet.of());

        cleanupTask.cellsCleanedUp(tx, ImmutableSet.of(CELL_1));
        verify(deleter).deleteStreams(tx, ImmutableSet.of());
    }

    @Test
    public void deletesPreciselyStreamsReturnedByFilterWhenDeletingMultiple() {
        when(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2)))
                .thenReturn(ImmutableSet.of(IDENTIFIER_2));

        cleanupTask.cellsCleanedUp(tx, ImmutableSet.of(CELL_1, CELL_2));
        verify(deleter).deleteStreams(tx, ImmutableSet.of(IDENTIFIER_2));
    }
}
