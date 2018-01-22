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

import static com.palantir.atlasdb.stream.GenericStreamStore.BLOCK_SIZE_IN_BYTES;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class SchemalessStreamStoreDeleter {
    private static final Logger log = LoggerFactory.getLogger(SchemalessStreamStoreDeleter.class);

    private final Namespace namespace;
    private final String streamStoreShortName;
    private final GenericStreamStoreCellCreator cellCreator;
    private final StreamStoreMetadataReader metadataReader;

    public SchemalessStreamStoreDeleter(
            Namespace namespace,
            String streamStoreShortName,
            StreamStoreCleanupMetadata cleanupMetadata) {
        this.namespace = namespace;
        this.streamStoreShortName = streamStoreShortName;
        this.cellCreator = new GenericStreamStoreCellCreator(cleanupMetadata);
        this.metadataReader = new StreamStoreMetadataReader(getTableReference(StreamTableType.METADATA), cellCreator);
    }

    public void deleteStreams(Transaction tx, Set<GenericStreamIdentifier> streamIds) {
        if (streamIds.isEmpty()) {
            log.debug("deleteStreams() was called with no identifiers, so we are returning.");
            return;
        }

        // Safe, because these are longs.
        log.info("Now attempting to delete streams from the {} stream store, with identifiers {}",
                LoggingArgs.tableRef(getTableReference(StreamTableType.VALUE)),
                SafeArg.of("streamIds", streamIds));

        Map<GenericStreamIdentifier, StreamMetadata> metadataInDb = metadataReader.readMetadata(tx, streamIds);

        Set<Cell> valueTableCellsToDelete = Sets.newHashSet();
        Set<Cell> hashTableCellsToDelete = Sets.newHashSet();
        Set<Cell> metadataTableCellsToDelete = Sets.newHashSet();

        for (Map.Entry<GenericStreamIdentifier, StreamMetadata> entry : metadataInDb.entrySet()) {
            GenericStreamIdentifier streamId = entry.getKey();
            StreamMetadata streamMetadata = entry.getValue();

            valueTableCellsToDelete.addAll(cellCreator.constructValueTableCellSet(
                    streamId, getNumberOfBlocksFromMetadata(entry.getValue())));
            metadataTableCellsToDelete.add(cellCreator.constructMetadataTableCell(streamId));
            hashTableCellsToDelete.add(cellCreator.constructHashTableCell(streamId, streamMetadata.getHash()));
        }

        transactionallyDeleteCells(tx, StreamTableType.VALUE, valueTableCellsToDelete);
        transactionallyDeleteCells(tx, StreamTableType.METADATA, metadataTableCellsToDelete);
        transactionallyDeleteCells(tx, StreamTableType.HASH, hashTableCellsToDelete);

        log.info("Cells to be deleted were {} from the {} table, {} from the {} table and {} from the {} table.",
                UnsafeArg.of("valueCells", valueTableCellsToDelete),
                LoggingArgs.tableRef("valueTable", getTableReference(StreamTableType.VALUE)),
                UnsafeArg.of("metadataCells", metadataTableCellsToDelete),
                LoggingArgs.tableRef("metadataTable", getTableReference(StreamTableType.METADATA)),
                UnsafeArg.of("hashCells", hashTableCellsToDelete),
                LoggingArgs.tableRef("hashTable", getTableReference(StreamTableType.HASH)));
    }

    private void transactionallyDeleteCells(Transaction tx, StreamTableType streamTableType, Set<Cell> cellsToDelete) {
        tx.delete(getTableReference(streamTableType), cellsToDelete);
    }

    private TableReference getTableReference(StreamTableType type) {
        return TableReference.create(namespace, type.getTableName(streamStoreShortName));
    }

    private long getNumberOfBlocksFromMetadata(StreamPersistence.StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }
}
