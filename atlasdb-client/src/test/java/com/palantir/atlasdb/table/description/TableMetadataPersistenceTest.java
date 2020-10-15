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

import com.google.common.collect.Lists;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.schema.stream.StreamStoreDefinitionBuilder;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@SuppressWarnings("checkstyle:all") // too many warnings to fix
public class TableMetadataPersistenceTest {

    private static final int CUSTOM_COMPRESSION_BLOCK_SIZE = 32;
    private static final int STREAM_STORE_BLOCK_SIZE_WITH_COMPRESS_IN_DB = 256;
    private static final int UNSET_BLOCK_SIZE = 0;

    private final TableDefinition tableDefinition;
    private final int compressionBlockSizeKB;

    @Parameters
    public static Collection<Object[]> testCases() {
        Collection<Object[]> params = Lists.newArrayList();

        params.add(new Object[] {getRangeScanWithoutCompression(), UNSET_BLOCK_SIZE});
        params.add(new Object[] {getDefaultExplicit(), AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB});
        params.add(new Object[] {getDefaultRangeScanExplicit(), AtlasDbConstants.DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB});
        params.add(new Object[] {getCustomExplicitCompression(), CUSTOM_COMPRESSION_BLOCK_SIZE});
        params.add(new Object[] {getCustomTable(), CUSTOM_COMPRESSION_BLOCK_SIZE});
        params.add(new Object[] {getStreamStoreTableWithCompressInDb(), STREAM_STORE_BLOCK_SIZE_WITH_COMPRESS_IN_DB});
        params.add(new Object[] {getStreamStoreTableDefault(), UNSET_BLOCK_SIZE});

        return params;
    }

    public TableMetadataPersistenceTest(TableDefinition tableDefinition, int compressionBlockSizeKB) {
        this.tableDefinition = tableDefinition;
        this.compressionBlockSizeKB = compressionBlockSizeKB;
    }

    @Test
    public void testSerializeAndDeserialize() {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        byte[] metadataAsBytes = metadata.persistToBytes();
        TableMetadata metadataFromBytes = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataAsBytes);
        Assert.assertEquals(metadata, metadataFromBytes);
    }

    @Test
    public void testMetadataHasExpectedCompressionBlockSize() {
        TableMetadata metadata = tableDefinition.toTableMetadata();
        Assert.assertEquals(compressionBlockSizeKB, metadata.getExplicitCompressionBlockSizeKB());
    }

    private static TableDefinition getRangeScanWithoutCompression() {
        return new TableDefinition() {{
            javaTableName("RangeScanWithoutCompression");

            rowName();
            rowComponent("component1", ValueType.STRING);

            columns();
            column("column1", "c", ValueType.VAR_LONG);

            rangeScanAllowed();
        }};
    }

    private static TableDefinition getDefaultExplicit() {
        return new TableDefinition() {{
            javaTableName("DefaultTableWithCompression");

            rowName();
            rowComponent("component1", ValueType.STRING);

            columns();
            column("column1", "c", ValueType.VAR_LONG);

            explicitCompressionRequested();
        }};
    }

    private static TableDefinition getDefaultRangeScanExplicit() {
        return new TableDefinition() {{
            javaTableName("RangeScanWithCompression");

            rowName();
            rowComponent("component1", ValueType.STRING);

            columns();
            column("column1", "c", ValueType.VAR_LONG);

            rangeScanAllowed();
            explicitCompressionRequested();
        }};
    }

    private static TableDefinition getCustomExplicitCompression() {
        return new TableDefinition() {{
            javaTableName("CustomExplicitCompression");

            rowName();
            rowComponent("component1", ValueType.STRING);

            columns();
            column("column1", "c", ValueType.VAR_LONG);

            explicitCompressionBlockSizeKB(CUSTOM_COMPRESSION_BLOCK_SIZE);
        }};
    }

    private static TableDefinition getCustomTable() {
        return new TableDefinition() {{
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
        }};
    }

    private static TableDefinition getStreamStoreTableWithCompressInDb() {
        return new StreamStoreDefinitionBuilder("t", "test", ValueType.VAR_LONG).compressBlocksInDb()
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
