/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description;

import org.immutables.value.Value;

import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.TableMetadata.Builder;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

@Value.Immutable
@Value.Style(get = {"get*", "is*", "has*"})
public abstract class TableMetadata implements Persistable {

    @Value.Default
    public NameMetadataDescription getRowMetadata() {
        return new NameMetadataDescription();
    }

    @Value.Default
    public ColumnMetadataDescription getColumns() {
        return new ColumnMetadataDescription();
    }

    @Value.Default
    public ConflictHandler getConflictHandler() {
        return ConflictHandler.RETRY_ON_WRITE_WRITE;
    }

    @Value.Default
    public CachePriority getCachePriority() {
        return CachePriority.WARM;
    }

    @Value.Default
    public boolean isRangeScanAllowed() {
        return false;
    }

    @Value.Default
    public int getExplicitCompressionBlockSizeKB() {
        return 0;
    }

    @Value.Default
    public boolean hasNegativeLookups() {
        return false;
    }

    @Value.Default
    public SweepStrategy getSweepStrategy() {
        return SweepStrategy.CONSERVATIVE;
    }

    @Value.Default
    public boolean isAppendHeavyAndReadLight() {
        return false;
    }

    @Value.Default
    public LogSafety getNameLogSafety() {
        return LogSafety.UNSAFE;
    }

    public static TableMetadata allDefault() {
        return builder().build();
    }

    public static class NiceBuilder extends ImmutableTableMetadata.Builder {
        public NiceBuilder test() {
            this.nameLogSafety(LogSafety.UNSAFE);
            return this;
        }
    }

    public static ImmutableTableMetadata.Builder builder() {
        return ImmutableTableMetadata.builder();
    }

    public static ImmutableTableMetadata.Builder internal() {
        return builder().conflictHandler(ConflictHandler.IGNORE_ALL).nameLogSafety(LogSafety.SAFE);
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
        builder.setRowName(getRowMetadata().persistToProto());
        builder.setColumns(getColumns().persistToProto());
        builder.setCachePriority(getCachePriority());
        builder.setRangeScanAllowed(isRangeScanAllowed());
        if (getExplicitCompressionBlockSizeKB() != 0) {
            builder.setExplicitCompressionBlockSizeKiloBytes(getExplicitCompressionBlockSizeKB());
        }
        builder.setNegativeLookups(hasNegativeLookups());
        builder.setSweepStrategy(getSweepStrategy());
        builder.setAppendHeavyAndReadLight(isAppendHeavyAndReadLight());
        builder.setNameLogSafety(getNameLogSafety());
        return builder;
    }

    public static TableMetadata hydrateFromProto(TableMetadataPersistence.TableMetadata message) {
        ImmutableTableMetadata.Builder builder = builder()
                .rowMetadata(NameMetadataDescription.hydrateFromProto(message.getRowName()))
                .columns(ColumnMetadataDescription.hydrateFromProto(message.getColumns()))
                .conflictHandler(ConflictHandlers.hydrateFromProto(message.getConflictHandler()));

        if (message.hasCachePriority()) {
            builder.cachePriority(message.getCachePriority());
        }
        if (message.hasRangeScanAllowed()) {
            builder.rangeScanAllowed(message.getRangeScanAllowed());
        }
        if (message.hasExplicitCompressionBlockSizeKiloBytes()) {
            builder.explicitCompressionBlockSizeKB(message.getExplicitCompressionBlockSizeKiloBytes());
        }
        if (message.hasNegativeLookups()) {
            builder.negativeLookups(message.getNegativeLookups());
        }
        if (message.hasSweepStrategy()) {
            builder.sweepStrategy(message.getSweepStrategy());
        }
        if (message.hasAppendHeavyAndReadLight()) {
            builder.appendHeavyAndReadLight(message.getAppendHeavyAndReadLight());
        }
        if (message.hasNameLogSafety()) {
            builder.nameLogSafety(message.getNameLogSafety());
        }

        return builder.build();
    }

    @Override
    public String toString() {
        return "TableMetadata ["
                + "rowMetadata=" + getRowMetadata()
                + ", columns=" + getColumns()
                + ", conflictHandler=" + getConflictHandler()
                + ", rowMetadata =" + getRowMetadata()
                + ", rangeScanAllowed =" + isRangeScanAllowed()
                + ", explicitCompressionBlockSizeKB =" + getExplicitCompressionBlockSizeKB()
                + ", negativeLookups = " + hasNegativeLookups()
                + ", sweepStrategy = " + getSweepStrategy()
                + ", appendHeavyAndReadLight = " + isAppendHeavyAndReadLight()
                + ", nameLogSafety = " + getNameLogSafety()
                + "]";
    }
}
