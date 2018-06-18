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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.util.crypto.Sha256Hash;

public class GenericStreamStoreRowCreatorTest {
    private static final GenericStreamIdentifier IDENTIFIER = ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG,
            ValueType.FIXED_LONG.convertFromJava(1L));

    private static final byte[] ENCODED_IDENTIFIER = {-114, -58, -91, 70, 112, 92, 76, 40};
    private static final byte[] ENCODED_IDENTIFIER_AND_BLOCK_ONE = {-35, 121, 15, -77, -86, -93, 113, -107};

    private static final byte[] FIXED_LONG_ONE = ValueType.FIXED_LONG.convertFromJava(1L);
    private static final byte[] VAR_LONG_ONE = ValueType.VAR_LONG.convertFromJava(1L);

    private final GenericStreamStoreRowCreator rowCreator = getRowCreator(1);

    @Test
    public void constructValueTableRowCanIncorporateOneHashedRowComponent() {
        assertThat(rowCreator.constructValueTableRow(IDENTIFIER, 1))
                .isEqualTo(EncodingUtils.add(ENCODED_IDENTIFIER, FIXED_LONG_ONE, VAR_LONG_ONE));
    }

    @Test
    public void constructValueTableRowCanIncorporateTwoHashedRowComponents() {
        assertThat(getRowCreator(2).constructValueTableRow(IDENTIFIER, 1))
                .isEqualTo(EncodingUtils.add(ENCODED_IDENTIFIER_AND_BLOCK_ONE, FIXED_LONG_ONE, VAR_LONG_ONE));
    }

    @Test
    public void constructValueTableRowCanIncorporateZeroHashedRowComponents() {
        assertThat(getRowCreator(0).constructValueTableRow(IDENTIFIER, 1))
                .isEqualTo(EncodingUtils.add(FIXED_LONG_ONE, VAR_LONG_ONE));
    }

    @Test
    public void constructIndexOrMetadataTableRowCanIncorporateHashedRowComponent() {
        assertThat(rowCreator.constructIndexOrMetadataTableRow(IDENTIFIER))
                .isEqualTo(EncodingUtils.add(ENCODED_IDENTIFIER, FIXED_LONG_ONE));
    }

    @Test
    public void constructIndexOrMetadataTableRowCorrectForCreatorWithTwoHashedRowComponents() {
        assertThat(getRowCreator(2).constructIndexOrMetadataTableRow(IDENTIFIER))
                .isEqualTo(EncodingUtils.add(ENCODED_IDENTIFIER, FIXED_LONG_ONE));
    }

    @Test
    public void constructIndexOrMetadataTableRowCanIncorporateNoHashedRowComponent() {
        assertThat(getRowCreator(0).constructIndexOrMetadataTableRow(IDENTIFIER))
                .isEqualTo(EncodingUtils.add(FIXED_LONG_ONE));
    }

    @Test
    public void constructHashTableRowIsResilientToEmptyByteString() {
        assertThat(rowCreator.constructHashTableRow(ByteString.EMPTY)).isEqualTo(Sha256Hash.EMPTY.getBytes());
    }

    @Test
    public void constructHashTableRowBuildsSha256HashFromByteString() throws IOException {
        Sha256Hash hash = Sha256Hash.createFrom(new ByteArrayInputStream(PtBytes.toBytes("hashbrowns")));
        ByteString byteString = ByteString.copyFrom(hash.getBytes());

        assertThat(rowCreator.constructHashTableRow(byteString)).isEqualTo(hash.getBytes());
    }

    private GenericStreamStoreRowCreator getRowCreator(int numHashedComponents) {
        return new GenericStreamStoreRowCreator(
                ImmutableStreamStoreCleanupMetadata.builder()
                        .numHashedRowComponents(numHashedComponents)
                        .streamIdType(ValueType.FIXED_LONG)
                        .build());
    }
}
