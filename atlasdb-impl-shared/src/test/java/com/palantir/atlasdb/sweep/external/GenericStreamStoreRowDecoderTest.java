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

import java.util.stream.IntStream;

import org.junit.Test;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.table.description.ValueType;

public class GenericStreamStoreRowDecoderTest {
    private static final ImmutableStreamStoreCleanupMetadata METADATA_ZERO_HASHED_COMPONENTS =
            ImmutableStreamStoreCleanupMetadata.builder()
                    .numHashedRowComponents(0)
                    .streamIdType(ValueType.VAR_SIGNED_LONG)
                    .build();
    private static final StreamStoreCleanupMetadata METADATA_ONE_HASHED_COMPONENT =
            ImmutableStreamStoreCleanupMetadata.builder()
                    .numHashedRowComponents(1)
                    .streamIdType(ValueType.VAR_LONG)
                    .build();
    private static final StreamStoreCleanupMetadata METADATA_TWO_HASHED_COMPONENTS =
            ImmutableStreamStoreCleanupMetadata.builder()
                    .numHashedRowComponents(2)
                    .streamIdType(ValueType.FIXED_LONG)
                    .build();
    private static final int HASH_LENGTH = StreamStoreHashEncodingUtils.getHashComponentBytes();

    @Test
    public void decodesRowKeysWithZeroHashedComponents() {
        assertThat(new GenericStreamStoreRowDecoder(METADATA_ZERO_HASHED_COMPONENTS)
                .decodeIndexOrMetadataTableRow(generateIncreasingByteArray(0, 40)))
                .isEqualTo(ImmutableGenericStreamIdentifier.of(
                        ValueType.VAR_SIGNED_LONG, generateIncreasingByteArray(0, 40)));
    }

    @Test
    public void decodesRowKeysWithOneHashedComponent() {
        assertThat(new GenericStreamStoreRowDecoder(METADATA_ONE_HASHED_COMPONENT)
                .decodeIndexOrMetadataTableRow(generateIncreasingByteArray(0, 79)))
                .isEqualTo(ImmutableGenericStreamIdentifier.of(ValueType.VAR_LONG,
                        generateIncreasingByteArray(HASH_LENGTH, 79)));
    }

    @Test
    public void decodesRowKeysWithTwoHashedComponents() {
        assertThat(new GenericStreamStoreRowDecoder(METADATA_TWO_HASHED_COMPONENTS)
                .decodeIndexOrMetadataTableRow(generateIncreasingByteArray(0, 111)))
                .isEqualTo(ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG,
                        generateIncreasingByteArray(HASH_LENGTH, 111)));
    }

    @Test
    public void throwsIfRowComponentIsTooShortToHaveHashedComponentsAndDecoderHasHashedComponents() {
        assertThatThrownBy(() -> new GenericStreamStoreRowDecoder(METADATA_TWO_HASHED_COMPONENTS)
                .decodeIndexOrMetadataTableRow(generateIncreasingByteArray(0, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void doesNotThrowIfRowComponentIsTooShortToHaveHashedComponentsButDecoderHasNoHashedComponents() {
        assertThat(new GenericStreamStoreRowDecoder(METADATA_ZERO_HASHED_COMPONENTS)
                .decodeIndexOrMetadataTableRow(generateIncreasingByteArray(0, HASH_LENGTH - 1)))
                .isEqualTo(ImmutableGenericStreamIdentifier.of(
                        ValueType.VAR_SIGNED_LONG, generateIncreasingByteArray(0, HASH_LENGTH - 1)));
    }

    private static byte[] generateIncreasingByteArray(int lowerLimit, int upperLimit) {
        Preconditions.checkArgument(Byte.MIN_VALUE < lowerLimit,
                "lower limit of %s is too low (byte minimmum: %s)",
                lowerLimit,
                Byte.MIN_VALUE);
        Preconditions.checkArgument(lowerLimit < upperLimit,
                "lower limit of %s is higher than the upper limit of %s",
                lowerLimit,
                upperLimit);
        Preconditions.checkArgument(upperLimit < Byte.MAX_VALUE,
                "upper limit of %s is too high (byte maximum: %s)",
                upperLimit,
                Byte.MAX_VALUE);

        int[] values = IntStream.range(lowerLimit, upperLimit).toArray();
        byte[] result = new byte[values.length];

        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
