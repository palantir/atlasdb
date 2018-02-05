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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.schema.cleanup.ImmutableStreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class SchemalessStreamStoreDeleterTest {
    private static final Namespace NAMESPACE = Namespace.DEFAULT_NAMESPACE;
    private static final String SHORT_NAME = "s";

    private final SchemalessStreamStoreDeleter deleter = new SchemalessStreamStoreDeleter(
            NAMESPACE,
            SHORT_NAME,
            ImmutableStreamStoreCleanupMetadata.builder()
                    .numHashedRowComponents(1)
                    .streamIdType(ValueType.VAR_LONG).build());

    @Test

    public void deleteStreamsDoesNotMakeDatabaseCallsIfDeletingAnEmptyStream() {
        Transaction tx = mock(Transaction.class);
        deleter.deleteStreams(tx, ImmutableSet.of());
        verifyNoMoreInteractions(tx);
    }

    @Test
    public void deleteStreamsWorksForSingleBlockStreams() {
        GenericStreamStoreCellCreator cellCreator = new GenericStreamStoreCellCreator(
                ImmutableStreamStoreCleanupMetadata.builder()
                        .numHashedRowComponents(1)
                        .streamIdType(ValueType.VAR_LONG)
                        .build());
        StreamPersistence.StreamMetadata streamMetadata = StreamPersistence.StreamMetadata.newBuilder()
                .setLength(1L)
                .setStatus(StreamPersistence.Status.STORED)
                .setHash(ByteString.copyFrom(new byte[32]))
                .build();
        Transaction tx = mock(Transaction.class);
        when(tx.get(any(), any())).thenReturn(
                ImmutableMap.of(cellCreator.constructMetadataTableCell(ImmutableGenericStreamIdentifier.of(ValueType.VAR_LONG, ValueType.VAR_LONG.convertFromJava(1L))),
                        streamMetadata.toByteArray()));

        deleter.deleteStreams(tx,
                ImmutableSet.of(
                        ImmutableGenericStreamIdentifier.of(ValueType.VAR_LONG, ValueType.VAR_LONG.convertFromJava(1L))
                ));
        verify(tx).delete(eq(StreamTableType.VALUE.getTableReference(NAMESPACE, SHORT_NAME)),
                eq(new GenericStreamStoreCellCreator(ImmutableStreamStoreCleanupMetadata.builder().numHashedRowComponents(1).streamIdType(ValueType.VAR_LONG).build())
                        .constructValueTableCellSet(ImmutableGenericStreamIdentifier.of(ValueType.VAR_LONG, ValueType.VAR_LONG.convertFromJava(1L)), 1)));
    }

    @Test
    public void deleteStreamsWorksForMultiBlockStreams() {
        foo
    }
}
