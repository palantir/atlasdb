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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class TableMetadataTest {
    private static final TableMetadata DEFAULT_TABLE_METADATA = TableMetadata.allDefault();
    private static final TableMetadata DIFFERENT_TABLE_METADATA = TableMetadata.builder()
            .conflictHandler(ConflictHandler.SERIALIZABLE)
            .cachePriority(TableMetadataPersistence.CachePriority.HOTTEST)
            .rangeScanAllowed(true)
            .explicitCompressionBlockSizeKB(32)
            .appendHeavyAndReadLight(true)
            .nameLogSafety(LogSafety.SAFE)
            .build();

    @Test
    public void nameIsNotLoggableByDefault() {
        assertThat(DEFAULT_TABLE_METADATA.getNameLogSafety()).isEqualTo(LogSafety.UNSAFE);
    }

    @Test
    public void canSerializeAndDeserializeDefaultMetadata() {
        assertCanSerializeAndDeserialize(DEFAULT_TABLE_METADATA);
    }

    @Test
    public void canSerializeAndDeserializeNonDefaultMetadata() {
        assertCanSerializeAndDeserialize(DIFFERENT_TABLE_METADATA);
    }

    @Test
    public void genericTableMetadataSerializedFormDidNotChangeWithImmutables() {
        // Generic table metadata serialized before TableMetadata switched to using immutables
        byte[] oldGenericTableMetadata = {10, 18, 10, 14, 10, 4, 110, 97, 109, 101, 16, 4, 24, 1, 32, 1, 48, 1, 24, 0,
                                          18, 30, 18, 28, 10, 18, 10, 14, 10, 4, 110, 97, 109, 101, 16, 4, 24, 1, 32, 1,
                                          48, 1, 24, 0, 18, 6, 8, 4, 24, 1, 32, 3, 24, 2, 32, 64, 48, 0, 64, 0, 72, 1,
                                          88, 0, 96, 0};
        assertThat(AtlasDbConstants.GENERIC_TABLE_METADATA).containsExactly(oldGenericTableMetadata);
    }

    @Test
    public void puncherStoreMetadataSerializedFormDidNotChangeWithImmutables() {
        // Puncher store metadata serialized before TableMetadata switched to using immutables
        byte[] oldPuncherStore = {10, 18, 10, 14, 10, 4, 116, 105, 109, 101, 16, 1, 24, 2, 32, 1, 48, 1, 24, 0, 18, 18,
                                  10, 16, 10, 1, 116, 18, 1, 116, 26, 6, 8, 1, 24, 1, 32, 3, 32, 1, 24, 1, 32, 64, 48,
                                  0, 64, 0, 72, 1, 88, 0, 96, 0};
        /**
         * see {@link KeyValueServicePuncherStore}
         */
        byte[] puncherStore = TableMetadata.internal()
                .rowMetadata(NameMetadataDescription.create("time", ValueType.VAR_LONG, TableMetadataPersistence.ValueByteOrder.DESCENDING))
                .columns(ColumnMetadataDescription.singleNamed("t", "t", ValueType.VAR_LONG))
                .build()
                .persistToBytes();
        assertThat(puncherStore).containsExactly(oldPuncherStore);
    }

    @Test
    public void coordiantionStoreMetadataSerializedFormDidNotChangeWithImmutables() {
        // Coordination store metadata serialized before TableMetadata switched to using immutables
        byte[] oldCoordinationStore = {10, 22, 10, 18, 10, 8, 115, 101, 113, 117, 101, 110, 99, 101, 16, 4, 24, 1, 32,
                                       1, 48, 0, 24, 0, 18, 40, 18, 38, 10, 28, 10, 24, 10, 14, 115, 101, 113, 117, 101,
                                       110, 99, 101, 78, 117, 109, 98, 101, 114, 16, 1, 24, 1, 32, 1, 48, 0, 24, 0, 18,
                                       6, 8, 4, 24, 1, 32, 3, 24, 1, 32, 64, 48, 0, 64, 0, 72, 0, 88, 0, 96, 0};
        /**
         * see {@link KeyValueServiceCoordinationStore}
         */
        byte[] coordinationStore = TableMetadata.internal()
                .rowMetadata(NameMetadataDescription.safe("sequence", ValueType.BLOB))
                .columns(ColumnMetadataDescription.singleDynamicSafe("sequenceNumber", ValueType.VAR_LONG, ValueType.BLOB))
                .sweepStrategy(TableMetadataPersistence.SweepStrategy.NOTHING)
                .build().persistToBytes();
        assertThat(coordinationStore).containsExactly(oldCoordinationStore);
    }

    private static void assertCanSerializeAndDeserialize(TableMetadata tableMetadata) {
        TableMetadataPersistence.TableMetadata.Builder builder = tableMetadata.persistToProto();
        assertThat(TableMetadata.hydrateFromProto(builder.build())).isEqualTo(tableMetadata);
    }

}
