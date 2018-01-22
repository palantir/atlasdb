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

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.StreamPersistence.StreamMetadata;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.common.collect.Maps2;
import com.palantir.logsafe.UnsafeArg;

public class StreamStoreMetadataReader {
    private static final Logger log = LoggerFactory.getLogger(StreamStoreMetadataReader.class);

    private final TableReference streamStoreMetadataTableRef;
    private final GenericStreamStoreCellCreator cellCreator;

    public StreamStoreMetadataReader(TableReference tableRef, GenericStreamStoreCellCreator cellCreator) {
        Preconditions.checkArgument(
                StreamTableType.isStreamStoreTableOfSpecificType(StreamTableType.METADATA, tableRef),
                "Table with reference %s does not appear to be a stream store metadata table",
                tableRef.getQualifiedName());
        this.streamStoreMetadataTableRef = tableRef;
        this.cellCreator = cellCreator;
    }

    public Map<GenericStreamIdentifier, StreamMetadata> readMetadata(Transaction tx,
            Set<GenericStreamIdentifier> streamIds) {
        Map<GenericStreamIdentifier, Cell> streamIdsToCells = streamIds.stream()
                .collect(Collectors.toMap(id -> id, cellCreator::constructMetadataTableCell));

        Map<Cell, byte[]> rawStreamMetadataMap = tx.get(streamStoreMetadataTableRef,
                ImmutableSet.copyOf(streamIdsToCells.values()));

        Map<Cell, StreamMetadata> streamMetadataMap = parseStreamMetadata(rawStreamMetadataMap);

        return Maps2.innerJoin(streamIdsToCells, streamMetadataMap);
    }

    @VisibleForTesting
    Map<Cell, StreamMetadata> parseStreamMetadata(Map<Cell, byte[]> rawStreamMetadataMap) {
        Map<Cell, StreamMetadata> result = Maps.newHashMap();
        for (Map.Entry<Cell, byte[]> rawEntry : rawStreamMetadataMap.entrySet()) {
            try {
                StreamMetadata metadata = StreamMetadata.parseFrom(rawEntry.getValue());
                result.put(rawEntry.getKey(), metadata);
            } catch (InvalidProtocolBufferException e) {
                log.warn("Found unparseable stream metadata of {} for the cell {}. Skipping.",
                        UnsafeArg.of("streamMetadata", Arrays.toString(rawEntry.getValue())),
                        UnsafeArg.of("cell", rawEntry.getKey()));
            }
        }
        return result;
    }
}
