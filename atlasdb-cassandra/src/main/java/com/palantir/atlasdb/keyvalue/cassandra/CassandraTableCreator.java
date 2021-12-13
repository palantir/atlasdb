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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CompressionOptions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.thrift.Compression;
import org.apache.thrift.TException;

class CassandraTableCreator {
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;

    CassandraTableCreator(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    void createTables(Map<TableReference, byte[]> tableRefToMetadata) {
        try {
            clientPool.runWithRetry(client -> {
                for (Map.Entry<TableReference, byte[]> entry : tableRefToMetadata.entrySet()) {
                    CassandraKeyValueServices.runWithWaitingForSchemas(
                            () -> createTable(entry.getKey(), entry.getValue(), client),
                            config,
                            client,
                            "adding the column family for table " + entry.getKey() + " in a call to create tables");
                }
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void createTable(TableReference tableRef, byte[] metadata, CassandraClient client) throws TException {
        CqlQuery query = constructQuery(tableRef, metadata);
        client.execute_cql3_query(query, Compression.NONE, CassandraKeyValueServiceImpl.WRITE_CONSISTENCY);
    }

    private CqlQuery constructQuery(TableReference tableRef, byte[] metadata) {
        TableMetadata tableMetadata = CassandraKeyValueServices.getMetadataOrDefaultToGeneric(metadata);
        boolean appendHeavyReadLight = tableMetadata.isAppendHeavyAndReadLight();
        String keyspace = wrapInQuotes(config.getKeyspaceOrThrow());
        String internalTableName = wrapInQuotes(CassandraKeyValueServiceImpl.internalTableName(tableRef));

        String query = SchemaBuilder.createTable(keyspace, internalTableName)
                .ifNotExists()
                .addPartitionKey(CassandraConstants.ROW, DataType.blob())
                .addClusteringColumn(CassandraConstants.COLUMN, DataType.blob())
                .addClusteringColumn(CassandraConstants.TIMESTAMP, DataType.bigint())
                .addColumn(CassandraConstants.VALUE, DataType.blob())
                .withOptions()
                .bloomFilterFPChance(CassandraTableOptions.bloomFilterFpChance(tableMetadata))
                .caching(SchemaBuilder.KeyCaching.ALL, SchemaBuilder.noRows())
                .compactionOptions(getCompaction(appendHeavyReadLight))
                .compactStorage()
                .compressionOptions(getCompression(tableMetadata.getExplicitCompressionBlockSizeKB()))
                .dcLocalReadRepairChance(0.0)
                .gcGraceSeconds(config.gcGraceSeconds())
                .minIndexInterval(CassandraTableOptions.minIndexInterval(tableMetadata))
                .maxIndexInterval(CassandraTableOptions.maxIndexInterval(tableMetadata))
                .speculativeRetry(SchemaBuilder.noSpeculativeRetry())
                .clusteringOrder(CassandraConstants.COLUMN, SchemaBuilder.Direction.ASC)
                .clusteringOrder(CassandraConstants.TIMESTAMP, SchemaBuilder.Direction.ASC)
                .buildInternal();

        return CqlQuery.builder()
                .safeQueryFormat(query + " AND id = '%s'")
                .addArgs(SafeArg.of("cfId", getUuidForTable(tableRef)))
                .build();
    }

    private String wrapInQuotes(String string) {
        return "\"" + string + "\"";
    }

    private CompactionOptions<?> getCompaction(boolean appendHeavyReadLight) {
        return appendHeavyReadLight
                ? SchemaBuilder.sizedTieredStategy().minThreshold(4).maxThreshold(32)
                : SchemaBuilder.leveledStrategy();
    }

    private CompressionOptions getCompression(int blockSize) {
        int chunkLength = blockSize != 0 ? blockSize : AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        return SchemaBuilder.lz4().withChunkLengthInKb(chunkLength);
    }

    private UUID getUuidForTable(TableReference tableRef) {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(tableRef);
        String keyspace = config.getKeyspaceOrThrow();
        String fullTableNameForUuid = keyspace + "." + internalTableName;
        return UUID.nameUUIDFromBytes(fullTableNameForUuid.getBytes(StandardCharsets.UTF_8));
    }
}
