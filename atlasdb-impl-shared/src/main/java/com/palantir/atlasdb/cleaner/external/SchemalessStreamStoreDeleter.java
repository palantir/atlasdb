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

package com.palantir.atlasdb.cleaner.external;

import static com.palantir.atlasdb.stream.GenericStreamStore.BLOCK_SIZE_IN_BYTES;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
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
    private final GenericStreamStoreRowHydrator rowHydrator;

    public SchemalessStreamStoreDeleter(
            Namespace namespace,
            String streamStoreShortName,
            StreamStoreCleanupMetadata cleanupMetadata) {
        this.namespace = namespace;
        this.streamStoreShortName = streamStoreShortName;
        this.cleanupMetadata = cleanupMetadata;
        this.rowHydrator = new GenericStreamStoreRowHydrator(cleanupMetadata);
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

        for (Map.Entry<Cell, byte[]> metadata : metadataInDb.entrySet()) {
            StreamPersistence.StreamMetadata streamMetadata = deserializeStreamMetadata(metadata.getValue());
            byte[] streamId = metadata.getKey().getRowName();
            long blocks = getNumberOfBlocksFromMetadata(streamMetadata);
            List<byte[]> valueTableRows = LongStream.range(0, blocks)
                    .boxed()
                    .map(blockId -> rowHydrator.constructValueTableRow(streamId, blockId))
                    .collect(Collectors.toList());

            ByteString hashString = streamMetadata.getHash();
            byte[] hashTableRow = rowHydrator.constructHashAidxTableRow(hashString);

        }
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
