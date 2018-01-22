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

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableDefinitionBuilder;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.transaction.api.Transaction;

public class StreamStoreMetadataReader {
    private final TableReference streamStoreMetadataTableRef;

    public StreamStoreMetadataReader(TableReference tableRef) {
        Preconditions.checkArgument(
                StreamTableType.isStreamStoreTableOfSpecificType(StreamTableType.METADATA, tableRef),
                "Table with reference %s does not appear to be a stream store metadata table",
                tableRef.getQualifiedName());
        this.streamStoreMetadataTableRef = tableRef;
    }

    public Map<Cell, byte[]> readMetadata(Transaction tx, Set<byte[]> streamIds) {
        Set<Cell> metadataCells = streamIds.stream()
                .map(id -> Cell.create(id,
                        PtBytes.toCachedBytes(StreamTableDefinitionBuilder.METADATA_COLUMN_SHORT_NAME)))
                .collect(Collectors.toSet());
        return tx.get(streamStoreMetadataTableRef, metadataCells);
    }
}
