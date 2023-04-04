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
import com.palantir.atlasdb.workload.store.IsolationLevel;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public final class AtlasDbUtils {

    private AtlasDbUtils() {}

    public static ConflictHandler toConflictHandler(IsolationLevel isolationLevel) {
        switch (isolationLevel) {
            case NONE:
                return ConflictHandler.IGNORE_ALL;
            case SNAPSHOT:
                return ConflictHandler.RETRY_ON_WRITE_WRITE;
            case SERIALIZABLE:
                return ConflictHandler.SERIALIZABLE;
            default:
                throw new SafeIllegalArgumentException(
                        "Cannot convert isolation level into a AtlasDb conflict handler due to unknown isolation"
                                + " level.",
                        SafeArg.of("isolationLevel", isolationLevel.name()));
        }
    }

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

    public static byte[] tableMetadata(IsolationLevel isolationLevel) {
        switch (isolationLevel) {
            case SERIALIZABLE:
                return tableMetadata(ConflictHandler.SERIALIZABLE);
            case SNAPSHOT:
                return tableMetadata(ConflictHandler.RETRY_ON_WRITE_WRITE);
            case NONE:
                return tableMetadata(ConflictHandler.IGNORE_ALL);
            default:
                throw new SafeIllegalStateException(
                        "Unknown isolation level", SafeArg.of("isolationLevel", isolationLevel));
        }
    }

    public static ConflictHandler toIndexConflictHandler(ConflictHandler baseTableConflictHandler) {
        return baseTableConflictHandler.checkReadWriteConflicts()
                ? ConflictHandler.SERIALIZABLE_INDEX
                : ConflictHandler.IGNORE_ALL;
    }

    public static byte[] indexMetadata(ConflictHandler baseTableConflictHandler) {
        return tableMetadata(toIndexConflictHandler(baseTableConflictHandler));
    }
}
