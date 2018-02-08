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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UnindexedStreamDeletionFilterTest {
    private static final byte[] ROW_1 = { 1 };
    private static final byte[] ROW_2 = { 12 };
    private static final byte[] COLUMN = { 2 };
    private static final Cell CELL_1 = Cell.create(ROW_1, COLUMN);
    private static final Cell CELL_2 = Cell.create(ROW_2, COLUMN);
    private static final GenericStreamIdentifier IDENTIFIER_1 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_1);
    private static final GenericStreamIdentifier IDENTIFIER_2 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_2);
    private static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test");

    private final Transaction tx = mock(Transaction.class);
    private final GenericStreamStoreRowDecoder rowDecoder = mock(GenericStreamStoreRowDecoder.class);
    private final UnindexedStreamDeletionFilter deletionFilter =
            new UnindexedStreamDeletionFilter(TABLE_REFERENCE, rowDecoder);

    @Before
    public void setUp() {
        when(rowDecoder.decodeIndexOrMetadataTableRow(ROW_1)).thenReturn(IDENTIFIER_1);
        when(rowDecoder.decodeIndexOrMetadataTableRow(ROW_2)).thenReturn(IDENTIFIER_2);
    }

    @Test
    public void returnsIdentifierIfNoLongerFoundInTable() {
        when(tx.getRows(any(), any(), any())).thenReturn(ImmutableSortedMap.of());

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1)))
                .isEqualTo(ImmutableSet.of(IDENTIFIER_1));
    }

    @Test
    public void doesNotReturnIdentifierIfFoundInTable() {
        when(tx.getRows(any(), any(), any())).thenReturn(
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(ROW_1, RowResult.of(CELL_1, new byte[0]))
                        .build());

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1)))
                .isEqualTo(ImmutableSet.of());
    }

    @Test
    public void returnsPreciselyIdentifiersNotFoundInTable() {
        when(tx.getRows(any(), any(), any())).thenReturn(
                ImmutableSortedMap.<byte[], RowResult<byte[]>>orderedBy(UnsignedBytes.lexicographicalComparator())
                        .put(ROW_1, RowResult.of(CELL_1, new byte[0]))
                        .build());

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2)))
                .isEqualTo(ImmutableSet.of(IDENTIFIER_2));
    }
}
