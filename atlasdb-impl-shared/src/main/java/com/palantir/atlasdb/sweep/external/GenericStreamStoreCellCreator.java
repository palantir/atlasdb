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
import java.util.stream.LongStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.schema.cleanup.StreamStoreCleanupMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableDefinitionBuilder;

public class GenericStreamStoreCellCreator {
    private static final Logger log = LoggerFactory.getLogger(GenericStreamStoreRowCreator.class);

    private final GenericStreamStoreRowCreator rowHydrator;

    public GenericStreamStoreCellCreator(StreamStoreCleanupMetadata cleanupMetadata) {
        this.rowHydrator = new GenericStreamStoreRowCreator(cleanupMetadata);
    }

    public Set<Cell> constructValueTableCellSet(byte[] streamId, long numBlocks) {
        return LongStream.range(0, numBlocks)
                .boxed()
                .map(blockId -> rowHydrator.constructValueTableRow(streamId, blockId))
                .map(rowName -> Cell.create(rowName,
                        PtBytes.toBytes(StreamTableDefinitionBuilder.VALUE_COLUMN_SHORT_NAME)))
                .collect(Collectors.toSet());
    }

    public Cell constructMetadataTableCell(byte[] streamId) {
        return Cell.create(rowHydrator.constructIndexOrMetadataTableRow(streamId),
                PtBytes.toBytes(StreamTableDefinitionBuilder.METADATA_COLUMN_SHORT_NAME));
    }

    public Cell constructHashTableCell(byte[] streamId, ByteString hash) {
        return Cell.create(rowHydrator.constructHashTableRow(hash), streamId);
    }
}
