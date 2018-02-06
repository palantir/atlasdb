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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.stream.GenericStreamStore;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

@RunWith(Parameterized.class)
public class SchemalessStreamStoreDeleterTest {
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;
    private static final String SHORT_NAME = "s";

    private static final ByteString HASH = ByteString.copyFrom(new byte[32]);

    private final GenericStreamIdentifier streamIdentifier;

    private final GenericStreamStoreCellCreator cellCreator;
    private final SchemalessStreamStoreDeleter deleter;

    private final Transaction tx = mock(Transaction.class);

    // Hacky. Unfortunately a transform on the objects for the naming doesn't seem to be easily allowed
    @Parameterized.Parameters(name = "{index} components hashed")
    public static Iterable<StreamStoreCleanupMetadata> streamStoreCleanupMetadata() {
        return ImmutableList.of(
                ImmutableStreamStoreCleanupMetadata.builder()
                        .numHashedRowComponents(0)
                        .streamIdType(ValueType.VAR_SIGNED_LONG)
                        .build(),
                ImmutableStreamStoreCleanupMetadata.builder()
                        .numHashedRowComponents(1)
                        .streamIdType(ValueType.VAR_LONG)
                        .build(),
                ImmutableStreamStoreCleanupMetadata.builder()
                        .numHashedRowComponents(2)
                        .streamIdType(ValueType.FIXED_LONG)
                        .build());
    }

    public SchemalessStreamStoreDeleterTest(StreamStoreCleanupMetadata cleanupMetadata) {
        this.streamIdentifier = ImmutableGenericStreamIdentifier.of(
                cleanupMetadata.streamIdType(), cleanupMetadata.streamIdType().convertFromJava(1L));
        this.cellCreator = new GenericStreamStoreCellCreator(cleanupMetadata);
        this.deleter = new SchemalessStreamStoreDeleter(NAMESPACE, SHORT_NAME, cleanupMetadata);
    }

    @Test
    public void deleteStreamsDoesNotMakeDatabaseCallsIfDeletingNothing() {
        deleter.deleteStreams(tx, ImmutableSet.of());
        verifyNoMoreInteractions(tx);
    }

    @Test
    public void deleteStreamsWorksForSingleBlockStreams() {
        when(tx.get(any(), any())).thenReturn(ImmutableMap.of(
                cellCreator.constructMetadataTableCell(streamIdentifier), getStreamMetadataForStreamOfLength(1L)));

        deleter.deleteStreams(tx, ImmutableSet.of(streamIdentifier));
        verifyValueTableDeletesCorrect(ImmutableMap.of(streamIdentifier, 1L));
        verifyMetadataTableAndHashTableDeletesCorrect(ImmutableSet.of(streamIdentifier));
    }

    @Test
    public void deleteStreamsWorksForMultiBlockStreams() {
        long numBlocks = 796;
        when(tx.get(any(), any())).thenReturn(ImmutableMap.of(
                cellCreator.constructMetadataTableCell(streamIdentifier),
                getStreamMetadataForStreamOfLength(numBlocks * GenericStreamStore.BLOCK_SIZE_IN_BYTES + 1)));

        deleter.deleteStreams(tx, ImmutableSet.of(streamIdentifier));
        verifyValueTableDeletesCorrect(ImmutableMap.of(streamIdentifier, numBlocks + 1));
        verifyMetadataTableAndHashTableDeletesCorrect(ImmutableSet.of(streamIdentifier));
    }

    @Test
    public void deletingMultipleStreamsTakesPlaceInOneTransaction() {
        GenericStreamIdentifier streamIdentifier2 = ImmutableGenericStreamIdentifier.builder()
                .from(streamIdentifier)
                .data(streamIdentifier.streamIdType().convertFromJava(2L))
                .build();
        when(tx.get(any(), any())).thenReturn(ImmutableMap.of(
                cellCreator.constructMetadataTableCell(streamIdentifier),
                getStreamMetadataForStreamOfLength(4 * GenericStreamStore.BLOCK_SIZE_IN_BYTES),
                cellCreator.constructMetadataTableCell(streamIdentifier2),
                getStreamMetadataForStreamOfLength(3 * GenericStreamStore.BLOCK_SIZE_IN_BYTES)));

        deleter.deleteStreams(tx, ImmutableSet.of(streamIdentifier, streamIdentifier2));
        verifyValueTableDeletesCorrect(ImmutableMap.of(streamIdentifier, 4L, streamIdentifier2, 3L));
        verifyMetadataTableAndHashTableDeletesCorrect(ImmutableSet.of(streamIdentifier, streamIdentifier2));
    }

    private byte[] getStreamMetadataForStreamOfLength(long length) {
        return StreamMetadata.newBuilder()
                .setLength(length)
                .setStatus(StreamPersistence.Status.STORED)
                .setHash(HASH)
                .build()
                .toByteArray();
    }

    private void verifyMetadataTableAndHashTableDeletesCorrect(Set<GenericStreamIdentifier> streamIdentifiers) {
        verifyDeleteCorrect(StreamTableType.METADATA,
                streamIdentifiers.stream()
                        .map(cellCreator::constructMetadataTableCell)
                        .collect(Collectors.toSet()));
        verifyDeleteCorrect(StreamTableType.HASH,
                streamIdentifiers.stream()
                        .map(identifier -> cellCreator.constructHashTableCell(identifier, HASH))
                        .collect(Collectors.toSet()));
    }

    private void verifyValueTableDeletesCorrect(Map<GenericStreamIdentifier, Long> identifierToNumBlocks) {
        Set<Cell> expected = identifierToNumBlocks.entrySet().stream()
                .flatMap(entry -> cellCreator.constructValueTableCellSet(entry.getKey(), entry.getValue()).stream())
                .collect(Collectors.toSet());
        verifyDeleteCorrect(StreamTableType.VALUE, expected);
    }

    @SuppressWarnings("unchecked") // Needed for ArgumentCaptor of a generic type
    private void verifyDeleteCorrect(StreamTableType tableType, Set<Cell> expected) {
        // Using Argument Captor because we don't want to enforce whether deletes are made at one shot, or if they
        // take place over multiple calls to tx.delete().
        ArgumentCaptor<Set<Cell>> valueCellCaptor = ArgumentCaptor.forClass((Class) Set.class);
        verify(tx, atLeastOnce()).delete(
                eq(tableType.getTableReference(NAMESPACE, SHORT_NAME)),
                valueCellCaptor.capture());
        Set<Cell> deletedCells = valueCellCaptor.getAllValues()
                .stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
        assertThat(deletedCells).isEqualTo(expected);
    }
}
