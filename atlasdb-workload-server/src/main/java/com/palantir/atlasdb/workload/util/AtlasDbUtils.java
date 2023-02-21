/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.workload.util;

import com.google.common.primitives.Ints;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.atlasdb.workload.store.WorkloadCell;

public final class AtlasDbUtils {

    private AtlasDbUtils() {}

    public static Cell toAtlasCell(WorkloadCell cell) {
        return Cell.create(Ints.toByteArray(cell.key()), Ints.toByteArray(cell.column()));
    }

    public static byte[] tableMetadata(ConflictHandler conflictHandler) {
        return new TableMetadata.Builder()
                .rowMetadata(new NameMetadataDescription())
                .columns(new ColumnMetadataDescription())
                .conflictHandler(conflictHandler)
                .cachePriority(TableMetadataPersistence.CachePriority.WARM)
                .rangeScanAllowed(true)
                .explicitCompressionBlockSizeKB(AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB)
                .negativeLookups(true)
                .sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
                .appendHeavyAndReadLight(false)
                .nameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                .build()
                .persistToBytes();
    }

    public static byte[] indexMetadata() {
        return new TableMetadata.Builder()
                .rowMetadata(new NameMetadataDescription())
                .columns(new ColumnMetadataDescription())
                .conflictHandler(ConflictHandler.SERIALIZABLE_INDEX)
                .cachePriority(TableMetadataPersistence.CachePriority.WARM)
                .rangeScanAllowed(true)
                .explicitCompressionBlockSizeKB(AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB)
                .negativeLookups(true)
                .sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
                .appendHeavyAndReadLight(false)
                .nameLogSafety(TableMetadataPersistence.LogSafety.SAFE)
                .build()
                .persistToBytes();
    }
}
