/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import javax.annotation.concurrent.Immutable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.TableMetadata.Builder;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

@Immutable
public class TableMetadata implements Persistable {
    final NameMetadataDescription rowMetadata;
    final ColumnMetadataDescription columns;
    final ConflictHandler conflictHandler;
    final CachePriority cachePriority;
    final boolean rangeScanAllowed;
    final int explicitCompressionBlockSizeKB;
    final boolean negativeLookups;
    final SweepStrategy sweepStrategy;
    final boolean appendHeavyAndReadLight;
    final LogSafety nameLogSafety;

    public TableMetadata() {
        this(LogSafety.UNSAFE);
    }

    public TableMetadata(LogSafety logSafety) {
        this(
                new NameMetadataDescription(),
                new ColumnMetadataDescription(),
                ConflictHandler.RETRY_ON_WRITE_WRITE,
                logSafety);
    }

    public TableMetadata(NameMetadataDescription rowMetadata,
                         ColumnMetadataDescription columns,
                         ConflictHandler conflictHandler) {
        this(
                rowMetadata,
                columns,
                conflictHandler,
                LogSafety.UNSAFE);
    }

    public TableMetadata(NameMetadataDescription rowMetadata,
            ColumnMetadataDescription columns,
            ConflictHandler conflictHandler,
            LogSafety logSafety) {
        this(
                rowMetadata,
                columns,
                conflictHandler,
                CachePriority.WARM,
                false,
                0,
                false,
                SweepStrategy.CONSERVATIVE,
                false,
                logSafety);
    }

    public TableMetadata(NameMetadataDescription rowMetadata,
                         ColumnMetadataDescription columns,
                         ConflictHandler conflictHandler,
                         CachePriority cachePriority,
                         boolean rangeScanAllowed,
                         int explicitCompressionBlockSizeKB,
                         boolean negativeLookups,
                         SweepStrategy sweepStrategy,
                         boolean appendHeavyAndReadLight) {
        this(
                rowMetadata,
                columns,
                conflictHandler,
                cachePriority,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                sweepStrategy,
                appendHeavyAndReadLight,
                LogSafety.UNSAFE);
    }

    public TableMetadata(NameMetadataDescription rowMetadata,
                         ColumnMetadataDescription columns,
                         ConflictHandler conflictHandler,
                         CachePriority cachePriority,
                         boolean rangeScanAllowed,
                         int explicitCompressionBlockSizeKB,
                         boolean negativeLookups,
                         SweepStrategy sweepStrategy,
                         boolean appendHeavyAndReadLight,
                         LogSafety nameLogSafety) {
        this.rowMetadata = rowMetadata;
        this.columns = columns;
        this.conflictHandler = conflictHandler;
        this.cachePriority = cachePriority;
        this.rangeScanAllowed = rangeScanAllowed;
        this.explicitCompressionBlockSizeKB = explicitCompressionBlockSizeKB;
        this.negativeLookups = negativeLookups;
        this.sweepStrategy = sweepStrategy;
        this.appendHeavyAndReadLight = appendHeavyAndReadLight;
        this.nameLogSafety = nameLogSafety;
    }

    public NameMetadataDescription getRowMetadata() {
        return rowMetadata;
    }

    public ColumnMetadataDescription getColumns() {
        return columns;
    }

    public boolean isWriteWriteConflict() {
        return conflictHandler != ConflictHandler.IGNORE_ALL;
    }

    public ConflictHandler getConflictHandler() {
        return conflictHandler;
    }

    public CachePriority getCachePriority() {
        return cachePriority;
    }

    public boolean isRangeScanAllowed() {
        return rangeScanAllowed;
    }

    public int getExplicitCompressionBlockSizeKB() {
        return explicitCompressionBlockSizeKB;
    }

    public boolean hasNegativeLookups() {
        return negativeLookups;
    }

    public SweepStrategy getSweepStrategy() {
        return sweepStrategy;
    }

    public boolean isAppendHeavyAndReadLight() {
        return appendHeavyAndReadLight;
    }

    public LogSafety getNameLogSafety() {
        return nameLogSafety;
    }

    @Override
    public byte[] persistToBytes() {
        return persistToProto().build().toByteArray();
    }

    public static final Hydrator<TableMetadata> BYTES_HYDRATOR = input -> {
        try {
            TableMetadataPersistence.TableMetadata message = TableMetadataPersistence.TableMetadata.parseFrom(input);
            return hydrateFromProto(message);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.throwUncheckedException(e);
        }
    };

    public TableMetadataPersistence.TableMetadata.Builder persistToProto() {
        Builder builder = TableMetadataPersistence.TableMetadata.newBuilder();
        builder.setConflictHandler(ConflictHandlers.persistToProto(getConflictHandler()));
        builder.setRowName(rowMetadata.persistToProto());
        builder.setColumns(columns.persistToProto());
        builder.setCachePriority(cachePriority);
        builder.setRangeScanAllowed(rangeScanAllowed);
        if (explicitCompressionBlockSizeKB != 0) {
            builder.setExplicitCompressionBlockSizeKiloBytes(explicitCompressionBlockSizeKB);
        }
        builder.setNegativeLookups(negativeLookups);
        builder.setSweepStrategy(sweepStrategy);
        // expiration strategy doesn't need to be persisted.
        builder.setAppendHeavyAndReadLight(appendHeavyAndReadLight);
        builder.setNameLogSafety(nameLogSafety);
        return builder;
    }

    public static TableMetadata hydrateFromProto(TableMetadataPersistence.TableMetadata message) {
        CachePriority cachePriority = CachePriority.WARM;
        if (message.hasCachePriority()) {
            cachePriority = message.getCachePriority();
        }
        boolean rangeScanAllowed = false;
        if (message.hasRangeScanAllowed()) {
            rangeScanAllowed = message.getRangeScanAllowed();
        }
        int explicitCompressionBlockSizeKB = 0;
        if (message.hasExplicitCompressionBlockSizeKiloBytes()) {
            explicitCompressionBlockSizeKB = message.getExplicitCompressionBlockSizeKiloBytes();
        }
        boolean negativeLookups = false;
        if (message.hasNegativeLookups()) {
            negativeLookups = message.getNegativeLookups();
        }
        SweepStrategy sweepStrategy = SweepStrategy.CONSERVATIVE;
        if (message.hasSweepStrategy()) {
            sweepStrategy = message.getSweepStrategy();
        }
        boolean appendHeavyAndReadLight = false;
        if (message.hasAppendHeavyAndReadLight()) {
            appendHeavyAndReadLight = message.getAppendHeavyAndReadLight();
        }
        LogSafety nameLogSafety = LogSafety.UNSAFE;
        if (message.hasNameLogSafety()) {
            nameLogSafety = message.getNameLogSafety();
        }

        return new TableMetadata(
                NameMetadataDescription.hydrateFromProto(message.getRowName()),
                ColumnMetadataDescription.hydrateFromProto(message.getColumns()),
                ConflictHandlers.hydrateFromProto(message.getConflictHandler()),
                cachePriority,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                sweepStrategy,
                appendHeavyAndReadLight,
                nameLogSafety);
    }

    @Override
    public String toString() {
        return "TableMetadata ["
                + "rowMetadata=" + rowMetadata
                + ", columns=" + columns
                + ", conflictHandler=" + conflictHandler
                + ", rowMetadata =" + rowMetadata
                + ", rangeScanAllowed =" + rangeScanAllowed
                + ", explicitCompressionBlockSizeKB =" + explicitCompressionBlockSizeKB
                + ", negativeLookups = " + negativeLookups
                + ", sweepStrategy = " + sweepStrategy
                + ", appendHeavyAndReadLight = " + appendHeavyAndReadLight
                + ", nameLogSafety = " + nameLogSafety
                + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cachePriority == null) ? 0 : cachePriority.hashCode());
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((conflictHandler == null) ? 0 : conflictHandler.hashCode());
        result = prime * result + (rangeScanAllowed ? 1231 : 1237);
        result = prime * result + ((rowMetadata == null) ? 0 : rowMetadata.hashCode());
        result = prime * result + (rangeScanAllowed ? 0 : 1);
        result = prime * result + explicitCompressionBlockSizeKB;
        result = prime * result + (negativeLookups ? 0 : 1);
        result = prime * result + sweepStrategy.hashCode();
        result = prime * result + (appendHeavyAndReadLight ? 0 : 1);
        result = prime * result + nameLogSafety.hashCode(); // Nonnull, because it has a default value
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TableMetadata other = (TableMetadata) obj;
        if (cachePriority != other.cachePriority) {
            return false;
        }
        if (columns == null) {
            if (other.columns != null) {
                return false;
            }
        } else if (!columns.equals(other.columns)) {
            return false;
        }
        if (conflictHandler != other.conflictHandler) {
            return false;
        }
        if (rangeScanAllowed != other.rangeScanAllowed) {
            return false;
        }
        if (rowMetadata == null) {
            if (other.rowMetadata != null) {
                return false;
            }
        } else if (!rowMetadata.equals(other.rowMetadata)) {
            return false;
        }
        if (explicitCompressionBlockSizeKB != other.explicitCompressionBlockSizeKB) {
            return false;
        }
        if (negativeLookups != other.negativeLookups) {
            return false;
        }
        if (sweepStrategy != other.sweepStrategy) {
            return false;
        }
        if (appendHeavyAndReadLight != other.appendHeavyAndReadLight) {
            return false;
        }
        if (nameLogSafety != other.nameLogSafety) {
            return false;
        }
        return true;
    }

}
