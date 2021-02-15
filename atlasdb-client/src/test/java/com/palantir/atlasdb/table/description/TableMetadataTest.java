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

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import org.junit.Test;

public class TableMetadataTest {
    private static final TableMetadata DEFAULT_TABLE_METADATA = TableMetadata.allDefault();
    private static final TableMetadata DIFFERENT_TABLE_METADATA = TableMetadata.builder()
            .conflictHandler(ConflictHandler.SERIALIZABLE)
            .cachePriority(TableMetadataPersistence.CachePriority.HOTTEST)
            .rangeScanAllowed(true)
            .explicitCompressionBlockSizeKB(32)
            .appendHeavyAndReadLight(true)
            .nameLogSafety(LogSafety.SAFE)
            .denselyAccessedWideRows(true)
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

    private static void assertCanSerializeAndDeserialize(TableMetadata tableMetadata) {
        TableMetadataPersistence.TableMetadata.Builder builder = tableMetadata.persistToProto();
        assertThat(TableMetadata.hydrateFromProto(builder.build())).isEqualTo(tableMetadata);
    }
}
