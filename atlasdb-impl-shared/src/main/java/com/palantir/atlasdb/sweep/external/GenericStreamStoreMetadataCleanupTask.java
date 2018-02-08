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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.cleaner.api.OnCleanupTask;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class GenericStreamStoreMetadataCleanupTask implements OnCleanupTask {
    private final GenericStreamStoreRowDecoder rowDecoder;
    private final StreamStoreMetadataReader metadataReader;
    private final SchemalessStreamStoreDeleter deleter;

    public GenericStreamStoreMetadataCleanupTask(
            TableReference tableToSweep,
            StreamStoreCleanupMetadata cleanupMetadata) {
        this.rowDecoder = new GenericStreamStoreRowDecoder(cleanupMetadata);
        this.metadataReader = new StreamStoreMetadataReader(
                tableToSweep,
                new GenericStreamStoreCellCreator(cleanupMetadata));
        this.deleter = new SchemalessStreamStoreDeleter(
                tableToSweep.getNamespace(),
                StreamTableType.getShortName(StreamTableType.METADATA, tableToSweep),
                cleanupMetadata);
    }

    @VisibleForTesting
    GenericStreamStoreMetadataCleanupTask(
            GenericStreamStoreRowDecoder rowDecoder,
            StreamStoreMetadataReader metadataReader,
            SchemalessStreamStoreDeleter deleter) {
        this.rowDecoder = rowDecoder;
        this.metadataReader = metadataReader;
        this.deleter = deleter;
    }

    @Override
    public boolean cellsCleanedUp(Transaction transaction, Set<Cell> cells) {
        Set<GenericStreamIdentifier> cellIds = extractStreamIdentifiers(cells);
        Set<GenericStreamIdentifier> unstoredStreamIdentifiers =
                filterStreamIdentifiersToUnstoredStreams(transaction, cellIds);
        deleter.deleteStreams(transaction, unstoredStreamIdentifiers);
        return false;
    }

    private Set<GenericStreamIdentifier> filterStreamIdentifiersToUnstoredStreams(Transaction transaction,
            Set<GenericStreamIdentifier> cellIds) {
        Map<GenericStreamIdentifier, StreamPersistence.StreamMetadata> metadataFromDb =
                metadataReader.readMetadata(transaction, cellIds);
        return metadataFromDb.entrySet().stream()
                .filter(entry -> entry.getValue().getStatus() != StreamPersistence.Status.STORED)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private Set<GenericStreamIdentifier> extractStreamIdentifiers(Set<Cell> cells) {
        return cells.stream()
                    .map(Cell::getRowName)
                    .map(rowDecoder::decodeIndexOrMetadataTableRow)
                    .collect(Collectors.toSet());
    }
}
