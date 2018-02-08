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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class UnstoredStreamDeletionFilterTest {
    private static final byte[] ROW_1 = { 1 };
    private static final byte[] ROW_2 = { 12 };
    private static final GenericStreamIdentifier IDENTIFIER_1 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_1);
    private static final GenericStreamIdentifier IDENTIFIER_2 =
            ImmutableGenericStreamIdentifier.of(ValueType.FIXED_LONG, ROW_2);

    private static final byte[] HASH = new byte[32];
    private static final StreamPersistence.StreamMetadata FAILED_STREAM_METADATA =
            StreamPersistence.StreamMetadata.newBuilder()
                    .setLength(3L)
                    .setHash(ByteString.copyFrom(HASH))
                    .setStatus(StreamPersistence.Status.FAILED)
                    .build();
    private static final StreamPersistence.StreamMetadata STORING_STREAM_METADATA =
            StreamPersistence.StreamMetadata.newBuilder()
                    .setLength(5L)
                    .setHash(ByteString.copyFrom(HASH))
                    .setStatus(StreamPersistence.Status.STORING)
                    .build();
    private static final StreamPersistence.StreamMetadata STORED_STREAM_METADATA =
            StreamPersistence.StreamMetadata.newBuilder()
                    .setLength(7L)
                    .setHash(ByteString.copyFrom(HASH))
                    .setStatus(StreamPersistence.Status.STORED)
                    .build();

    private final Transaction tx = mock(Transaction.class);
    private final StreamStoreMetadataReader metadataReader = mock(StreamStoreMetadataReader.class);
    private final GenericStreamDeletionFilter deletionFilter = new UnstoredStreamDeletionFilter(metadataReader);

    @Test
    public void filtersOutStreamsThatAreStored() {
        when(metadataReader.readMetadata(tx, ImmutableSet.of(IDENTIFIER_1))).thenReturn(
                ImmutableMap.of(IDENTIFIER_1, STORED_STREAM_METADATA));

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1)))
                .isEqualTo(ImmutableSet.of());
    }

    @Test
    public void retainsStreamsThatAreNotStored() {
        when(metadataReader.readMetadata(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2))).thenReturn(
                ImmutableMap.of(IDENTIFIER_1, STORING_STREAM_METADATA,
                        IDENTIFIER_2, FAILED_STREAM_METADATA));

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2)))
                .isEqualTo(ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2));
    }

    @Test
    public void retainsPreciselyStreamsThatAreNotStored() {
        when(metadataReader.readMetadata(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2))).thenReturn(
                ImmutableMap.of(IDENTIFIER_1, STORED_STREAM_METADATA,
                        IDENTIFIER_2, FAILED_STREAM_METADATA));

        assertThat(deletionFilter.getStreamIdentifiersToDelete(tx, ImmutableSet.of(IDENTIFIER_1, IDENTIFIER_2)))
                .isEqualTo(ImmutableSet.of(IDENTIFIER_2));
    }
}
