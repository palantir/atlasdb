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

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class GenericStreamStoreCleanupTask implements OnCleanupTask {
    private final GenericStreamStoreRowDecoder rowDecoder;
    private final GenericStreamDeletionFilter deletionFilter;
    private final SchemalessStreamStoreDeleter deleter;

    private GenericStreamStoreCleanupTask(
            StreamStoreCleanupMetadata cleanupMetadata,
            GenericStreamDeletionFilter deletionFilter,
            SchemalessStreamStoreDeleter deleter) {
        this.rowDecoder = new GenericStreamStoreRowDecoder(cleanupMetadata);
        this.deletionFilter = deletionFilter;
        this.deleter = deleter;
    }

    @VisibleForTesting
    GenericStreamStoreCleanupTask(
            GenericStreamStoreRowDecoder rowDecoder,
            GenericStreamDeletionFilter deletionFilter,
            SchemalessStreamStoreDeleter deleter) {
        this.rowDecoder = rowDecoder;
        this.deletionFilter = deletionFilter;
        this.deleter = deleter;
    }

    public static OnCleanupTask createForMetadataTables(
            TableReference metadataTableRef, StreamStoreCleanupMetadata cleanupMetadata) {
        return new GenericStreamStoreCleanupTask(
                cleanupMetadata,
                new UnstoredStreamDeletionFilter(metadataTableRef, cleanupMetadata),
                new SchemalessStreamStoreDeleter(
                        metadataTableRef.getNamespace(),
                        StreamTableType.getShortName(StreamTableType.METADATA, metadataTableRef),
                        cleanupMetadata));
    }

    @Override
    public boolean cellsCleanedUp(Transaction transaction, Set<Cell> cells) {
        Set<GenericStreamIdentifier> streamIdsFromCells = extractStreamIdentifiers(cells);
        Set<GenericStreamIdentifier> streamIdsToDelete =
                deletionFilter.getStreamIdentifiersToDelete(transaction, streamIdsFromCells);
        deleter.deleteStreams(transaction, streamIdsToDelete);
        return false;
    }

    private Set<GenericStreamIdentifier> extractStreamIdentifiers(Set<Cell> cells) {
        return cells.stream()
                    .map(Cell::getRowName)
                    .map(rowDecoder::decodeIndexOrMetadataTableRow)
                    .collect(Collectors.toSet());
    }
}
