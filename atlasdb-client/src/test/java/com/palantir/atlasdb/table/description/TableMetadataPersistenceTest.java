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

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class TableMetadataPersistenceTest {

    private static final int CUSTOM_COMPRESSION_BLOCK_SIZE = 32;
    private static final int STREAM_STORE_BLOCK_SIZE_WITH_COMPRESS_IN_DB = 256;
    private static final int UNSET_BLOCK_SIZE = 0;

    public static List<Arguments> getParameters() {
        List<Arguments> params = new ArrayList<>();

        params.add(Arguments.of(getRangeScanWithoutCompression(), UNSET_BLOCK_SIZE));
        params.add(Arguments.of(getDefaultExplicit(), AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB));
        params.add(Arguments.of(
                getDefaultRangeScanExplicit(),
                AtlasDbConstants.DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB));
        params.add(Arguments.of(getCustomExplicitCompression(), CUSTOM_COMPRESSION_BLOCK_SIZE));
        params.add(Arguments.of(getCustomTable(), CUSTOM_COMPRESSION_BLOCK_SIZE));
        params.add(Arguments.of(getStreamStoreTableWithCompressInDb(), STREAM_STORE_BLOCK_SIZE_WITH_COMPRESS_IN_DB));
        params.add(Arguments.of(getStreamStoreTableDefault(), UNSET_BLOCK_SIZE));

        return params;
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testSerializeAndDeserialize(TableDefinition tableDefinition) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        byte[] metadataAsBytes = metadata.persistToBytes();
        TableMetadata metadataFromBytes = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataAsBytes);
        assertThat(metadataFromBytes).isEqualTo(metadata);
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void testMetadataHasExpectedCompressionBlockSize(
            TableDefinition tableDefinition, int compressionBlockSizeKB) {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        assertThat(metadata.getExplicitCompressionBlockSizeKB()).isEqualTo(compressionBlockSizeKB);
    }

    private static TableDefinition getRangeScanWithoutCompression() {
        return new TableDefinition() {
            {
                javaTableName("RangeScanWithoutCompression");

                rowName();
                rowComponent("component1", ValueType.STRING);

                columns();
                column("column1", "c", ValueType.VAR_LONG);

                rangeScanAllowed();
            }
        };
    }

    private static TableDefinition getDefaultExplicit() {
        return new TableDefinition() {
            {
                javaTableName("DefaultTableWithCompression");

                rowName();
                rowComponent("component1", ValueType.STRING);

                columns();
                column("column1", "c", ValueType.VAR_LONG);

                explicitCompressionRequested();
            }
        };
    }

    private static TableDefinition getDefaultRangeScanExplicit() {
        return new TableDefinition() {
            {
                javaTableName("RangeScanWithCompression");

                rowName();
                rowComponent("component1", ValueType.STRING);

                columns();
                column("column1", "c", ValueType.VAR_LONG);

                rangeScanAllowed();
                explicitCompressionRequested();
            }
        };
    }

    private static TableDefinition getCustomExplicitCompression() {
        return new TableDefinition() {
            {
                javaTableName("CustomExplicitCompression");

                rowName();
                rowComponent("component1", ValueType.STRING);

                columns();
                column("column1", "c", ValueType.VAR_LONG);

                explicitCompressionBlockSizeKB(CUSTOM_COMPRESSION_BLOCK_SIZE);
            }
        };
    }

    private static TableDefinition getCustomTable() {
        return new TableDefinition() {
            {
                javaTableName("CustomTable");

                rowName();
                rowComponent("component1", ValueType.VAR_LONG, TableMetadataPersistence.ValueByteOrder.DESCENDING);
                rowComponent("component2", ValueType.FIXED_LONG_LITTLE_ENDIAN);

                columns();
                column("column1", "c", ValueType.UUID);
                column("column2", "d", ValueType.BLOB);

                // setting everything explicitly to test serialization
                conflictHandler(ConflictHandler.SERIALIZABLE);
                sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH);
                cachePriority(TableMetadataPersistence.CachePriority.COLD);
                explicitCompressionBlockSizeKB(CUSTOM_COMPRESSION_BLOCK_SIZE);
                negativeLookups();
                appendHeavyAndReadLight();
            }
        };
    }

    private static TableDefinition getStreamStoreTableWithCompressInDb() {
        return new StreamStoreDefinitionBuilder("t", "test", ValueType.VAR_LONG)
                .compressBlocksInDb()
                .build()
                .getTables()
                .get("t_stream_value");
    }

    private static TableDefinition getStreamStoreTableDefault() {
        return new StreamStoreDefinitionBuilder("t", "test", ValueType.VAR_LONG)
                .build()
                .getTables()
                .get("t_stream_value");
    }
}
