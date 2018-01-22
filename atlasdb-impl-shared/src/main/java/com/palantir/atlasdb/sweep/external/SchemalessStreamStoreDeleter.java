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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.StreamPersistence;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.logsafe.UnsafeArg;

public class SchemalessStreamStoreDeleter {
    private static final Logger log = LoggerFactory.getLogger(SchemalessStreamStoreDeleter.class);

    private final Namespace namespace;
    private final String streamStoreShortName;
    private final StreamStoreCleanupMetadata cleanupMetadata;
    private final GenericStreamStoreCellCreator cellHydrator;

    public SchemalessStreamStoreDeleter(
            Namespace namespace,
            String streamStoreShortName,
            StreamStoreCleanupMetadata cleanupMetadata) {
        this.namespace = namespace;
        this.streamStoreShortName = streamStoreShortName;
        this.cleanupMetadata = cleanupMetadata;
        this.cellHydrator = new GenericStreamStoreCellCreator(cleanupMetadata);
    }

    public void deleteStreams(Transaction tx, Set<byte[]> streamIds) {
        if (streamIds.isEmpty()) {
            return;
        }

        // "md" is part of the v1 stream store schema
        Set<Cell> metadataCells = streamIds.stream()
                .map(id -> Cell.create(id, PtBytes.toCachedBytes("md")))
                .collect(Collectors.toSet());

        // get metadata for the relevant streamIds
        Map<Cell, byte[]> metadataInDb =
                tx.get(TableReference.create(namespace, StreamTableType.METADATA.getTableName(streamStoreShortName)),
                        metadataCells);

        Set<Cell> valueTableCellsToDelete = Sets.newHashSet();
        Set<Cell> hashTableCellsToDelete = Sets.newHashSet();
        Set<Cell> metadataTableCellsToDelete = Sets.newHashSet();

        for (Map.Entry<Cell, byte[]> metadata : metadataInDb.entrySet()) {
            byte[] streamId = metadata.getKey().getRowName();

            StreamPersistence.StreamMetadata streamMetadata = deserializeStreamMetadata(metadata.getValue());
            long blocks = getNumberOfBlocksFromMetadata(streamMetadata);

            valueTableCellsToDelete.addAll(cellHydrator.constructValueTableCellSet(streamId, blocks));
            metadataTableCellsToDelete.add(cellHydrator.constructMetadataTableCell(streamId));
            hashTableCellsToDelete.add(cellHydrator.constructHashTableCell(streamId, streamMetadata.getHash()));
        }

        tx.delete(TableReference.create(namespace, StreamTableType.METADATA.getTableName(streamStoreShortName)),
                metadataTableCellsToDelete);
        tx.delete(TableReference.create(namespace, StreamTableType.VALUE.getTableName(streamStoreShortName)),
                valueTableCellsToDelete);
        tx.delete(TableReference.create(namespace, StreamTableType.HASH.getTableName(streamStoreShortName)),
                hashTableCellsToDelete);
    }

    private StreamPersistence.StreamMetadata deserializeStreamMetadata(byte[] value) {
        try {
            return StreamPersistence.StreamMetadata.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Attempted to deserialize stream metadata {}, but failed",
                    UnsafeArg.of("streamMetadata", value));
            throw Throwables.propagate(e);
        }
    }

    private long getNumberOfBlocksFromMetadata(StreamPersistence.StreamMetadata metadata) {
        return (metadata.getLength() + BLOCK_SIZE_IN_BYTES - 1) / BLOCK_SIZE_IN_BYTES;
    }
}
