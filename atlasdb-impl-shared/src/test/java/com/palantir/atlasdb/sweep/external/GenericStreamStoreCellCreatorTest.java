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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.stream.StreamTableDefinitionBuilder;
import com.palantir.atlasdb.table.description.ValueType;

public class GenericStreamStoreCellCreatorTest {
    private static final byte[] ROW_NAME_1 = { 33 };
    private static final byte[] ROW_NAME_2 = { 34 };
    private static final byte[] ROW_NAME_3 = { 35 };

    private static final byte[] IDENTIFIER = { 1, 2, 3, 4, 5 };
    private static final ByteString HASH = ByteString.copyFrom(new byte[] { 1, 2, 3 });
    private static final byte[] HASHED_NAME = { 4, 5, 6 };

    private static final GenericStreamIdentifier STREAM_IDENTIFIER = ImmutableGenericStreamIdentifier.builder()
            .streamIdType(ValueType.VAR_LONG)
            .data(IDENTIFIER)
            .build();

    private final GenericStreamStoreRowCreator rowCreator = mock(GenericStreamStoreRowCreator.class);
    private final GenericStreamStoreCellCreator cellCreator = new GenericStreamStoreCellCreator(rowCreator);

    @Test
    public void createsValueTableCellsCorrectlyForOneBlockStream() {
        when(rowCreator.constructValueTableRow(STREAM_IDENTIFIER, 0)).thenReturn(ROW_NAME_1);
        Set<Cell> cells = cellCreator.constructValueTableCellSet(STREAM_IDENTIFIER, 1);
        assertThat(cells).containsExactlyInAnyOrder(
                Cell.create(ROW_NAME_1, PtBytes.toCachedBytes(StreamTableDefinitionBuilder.VALUE_COLUMN_SHORT_NAME)));
    }

    @Test
    public void createsValueTableCellsCorrectlyForMultipleBlockStream() {
        when(rowCreator.constructValueTableRow(STREAM_IDENTIFIER, 0)).thenReturn(ROW_NAME_1);
        when(rowCreator.constructValueTableRow(STREAM_IDENTIFIER, 1)).thenReturn(ROW_NAME_2);
        when(rowCreator.constructValueTableRow(STREAM_IDENTIFIER, 2)).thenReturn(ROW_NAME_3);
        Set<Cell> cells = cellCreator.constructValueTableCellSet(STREAM_IDENTIFIER, 3);

        byte[] valueKey = PtBytes.toCachedBytes(StreamTableDefinitionBuilder.VALUE_COLUMN_SHORT_NAME);
        assertThat(cells).containsExactlyInAnyOrder(
                Cell.create(ROW_NAME_1, valueKey),
                Cell.create(ROW_NAME_2, valueKey),
                Cell.create(ROW_NAME_3, valueKey));
    }

    @Test
    public void returnsEmptySetForZeroBlockStream() {
        assertThat(cellCreator.constructValueTableCellSet(STREAM_IDENTIFIER, 0)).isEmpty();
    }

    @Test
    public void throwsForNegativeBlockStream() {
        assertThatThrownBy(() -> cellCreator.constructValueTableCellSet(STREAM_IDENTIFIER, -5))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void createsMetadataTableCellCorrectly() {
        when(rowCreator.constructIndexOrMetadataTableRow(STREAM_IDENTIFIER)).thenReturn(ROW_NAME_1);
        Cell metadataCell = cellCreator.constructMetadataTableCell(STREAM_IDENTIFIER);
        assertThat(metadataCell.getRowName()).isEqualTo(ROW_NAME_1);
        assertThat(metadataCell.getColumnName()).isEqualTo(
                PtBytes.toCachedBytes(StreamTableDefinitionBuilder.METADATA_COLUMN_SHORT_NAME));
    }

    @Test
    public void createsHashTableCellCorrectly() {
        when(rowCreator.constructHashTableRow(HASH)).thenReturn(HASHED_NAME);
        Cell hashCell = cellCreator.constructHashTableCell(STREAM_IDENTIFIER, HASH);
        assertThat(hashCell.getRowName()).isEqualTo(HASHED_NAME);
        assertThat(hashCell.getColumnName()).isEqualTo(STREAM_IDENTIFIER.data());
    }

}
