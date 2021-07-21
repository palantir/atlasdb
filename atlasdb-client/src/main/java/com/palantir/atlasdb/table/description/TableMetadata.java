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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@TableMetadataStyle
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

    /**
     * Returns whether the table has densely accessed wide rows. This helps identify tables which could benefit from
     * setting database tuning parameters to values more in line with such workflows, like the _transactions2 table.
     */
    @Value.Default
    public boolean hasDenselyAccessedWideRows() {
        return false;
    }

    public static TableMetadata allDefault() {
        return builder().build();
    }

    public static Builder internal() {
        return builder().conflictHandler(ConflictHandler.IGNORE_ALL).nameLogSafety(LogSafety.SAFE);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends ImmutableTableMetadata.Builder {
        public Builder singleRowComponent(String name, ValueType valueType) {
            this.rowMetadata(NameMetadataDescription.create(name, valueType));
            return this;
        }

        public Builder singleSafeRowComponent(String name, ValueType valueType) {
            this.rowMetadata(NameMetadataDescription.safe(name, valueType));
            return this;
        }

        public Builder singleNamedColumn(String shortName, String longName, ValueType valueType) {
            this.columns(new ColumnMetadataDescription(ImmutableList.of(
                    new NamedColumnDescription(shortName, longName, ColumnValueDescription.forType(valueType)))));
            return this;
        }

        public Builder singleDynamicColumn(String name, ValueType colType, ValueType valueType) {
            return dynamicColumns(ImmutableList.of(NameComponentDescription.of(name, colType)), valueType);
        }

        public Builder singleDynamicSafeColumn(String name, ValueType colType, ValueType valueType) {
            return dynamicColumns(ImmutableList.of(NameComponentDescription.safe(name, colType)), valueType);
        }

        public Builder dynamicColumns(List<NameComponentDescription> components, ValueType valueType) {
            this.columns(new ColumnMetadataDescription(new DynamicColumnDescription(
                    NameMetadataDescription.create(components), ColumnValueDescription.forType(valueType))));
            return this;
        }
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
        TableMetadataPersistence.TableMetadata.Builder builder = TableMetadataPersistence.TableMetadata.newBuilder();
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
        if (hasDenselyAccessedWideRows()) {
            builder.setDenselyAccessedWideRows(hasDenselyAccessedWideRows());
        }
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
        if (message.hasDenselyAccessedWideRows()) {
            builder.denselyAccessedWideRows(message.getDenselyAccessedWideRows());
        }

        return builder.build();
    }
}
