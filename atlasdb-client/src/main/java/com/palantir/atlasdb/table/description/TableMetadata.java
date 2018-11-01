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
public abstract class TableMetadata implements Persistable {

    @Value.Default
    public NameMetadataDescription rowMetadata() {
        return new NameMetadataDescription();
    }

    @Value.Default
    public ColumnMetadataDescription columns() {
        return new ColumnMetadataDescription();
    }

    @Value.Default
    public ConflictHandler conflictHandler() {
        return ConflictHandler.RETRY_ON_WRITE_WRITE;
    }

    @Value.Default
    public CachePriority cachePriority() {
        return CachePriority.WARM;
    }

    @Value.Default
    public boolean rangeScanAllowed() {
        return false;
    }

    @Value.Default
    public int explicitCompressionBlockSizeKB() {
        return 0;
    }

    @Value.Default
    public boolean negativeLookups() {
        return false;
    }

    @Value.Default
    public SweepStrategy sweepStrategy() {
        return SweepStrategy.CONSERVATIVE;
    }

    @Value.Default
    public boolean appendHeavyAndReadLight() {
        return false;
    }

    @Value.Default
    public LogSafety nameLogSafety() {
        return LogSafety.UNSAFE;
    }

    public static TableMetadata allDefault() {
        return builder().build();
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
        builder.setConflictHandler(ConflictHandlers.persistToProto(conflictHandler()));
        builder.setRowName(rowMetadata().persistToProto());
        builder.setColumns(columns().persistToProto());
        builder.setCachePriority(cachePriority());
        builder.setRangeScanAllowed(rangeScanAllowed());
        builder.setExplicitCompressionBlockSizeKiloBytes(explicitCompressionBlockSizeKB());
        builder.setNegativeLookups(negativeLookups());
        builder.setSweepStrategy(sweepStrategy());
        builder.setAppendHeavyAndReadLight(appendHeavyAndReadLight());
        builder.setNameLogSafety(nameLogSafety());
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
                + "rowMetadata=" + rowMetadata()
                + ", columns=" + columns()
                + ", conflictHandler=" + conflictHandler()
                + ", rowMetadata =" + rowMetadata()
                + ", rangeScanAllowed =" + rangeScanAllowed()
                + ", explicitCompressionBlockSizeKB =" + explicitCompressionBlockSizeKB()
                + ", negativeLookups = " + negativeLookups()
                + ", sweepStrategy = " + sweepStrategy()
                + ", appendHeavyAndReadLight = " + appendHeavyAndReadLight()
                + ", nameLogSafety = " + nameLogSafety()
                + "]";
    }
}
