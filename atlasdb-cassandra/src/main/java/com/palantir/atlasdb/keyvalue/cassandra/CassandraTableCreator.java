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

import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.Compression;
import org.apache.thrift.TException;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;

class CassandraTableCreator {
    private static final String ROW = "key";
    private static final String COLUMN = "column1";
    private static final String TIMESTAMP = "column2";
    private static final String VALUE = "value";
    private static final String CACHING_OPTION = "caching";
    private static final String KEYS_ONLY = "keys_only";
    private static final String POPULATE_IO_CACHE_ON_FLUSH_OPTION = "populate_io_cache_on_flush";
    private static final String COMPRESSION_OPTION = "compression";
    private static final String SSTABLE_COMPRESSION = "sstable_compression";
    private static final String LZ_4_COMPRESSOR = "LZ4Compressor";
    private static final String COMPRESSION_CHUNK_LENGTH_OPTION = "chunk_length_kb";
    private static final String NEVER_RETRY = "NONE";

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
                            () -> createTable(entry.getKey(), entry.getValue(), client), config, client,
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

        ImmutableMap<String, Object> compressionOptions = ImmutableMap.of(
                SSTABLE_COMPRESSION, LZ_4_COMPRESSOR,
                COMPRESSION_CHUNK_LENGTH_OPTION, getCompression(tableMetadata.getExplicitCompressionBlockSizeKB()));
        boolean shouldPopulateIoCacheOnFlush = tableMetadata.getCachePriority() == CachePriority.HOTTEST;

        SimpleStatement query = SchemaBuilder.createTable(keyspace, internalTableName)
                .ifNotExists()
                .withPartitionKey(ROW, DataTypes.BLOB)
                .withClusteringColumn(COLUMN, DataTypes.BLOB)
                .withClusteringColumn(TIMESTAMP, DataTypes.BIGINT)
                .withColumn(VALUE, DataTypes.BLOB)
                .withBloomFilterFpChance(CassandraTableOptions.bloomFilterFpChance(tableMetadata))
                .withOption(CACHING_OPTION, KEYS_ONLY)
                .withOption(POPULATE_IO_CACHE_ON_FLUSH_OPTION, shouldPopulateIoCacheOnFlush)
                .withCompaction(getCompaction(appendHeavyReadLight))
                .withOption(COMPRESSION_OPTION, compressionOptions)
                .withDcLocalReadRepairChance(0.1)
                .withGcGraceSeconds(config.gcGraceSeconds())
                .withMinIndexInterval(CassandraTableOptions.minIndexInterval(tableMetadata))
                .withMaxIndexInterval(CassandraTableOptions.maxIndexInterval(tableMetadata))
                .withSpeculativeRetry(NEVER_RETRY)
                .withClusteringOrder(COLUMN, ClusteringOrder.ASC)
                .withClusteringOrder(TIMESTAMP, ClusteringOrder.ASC)
                .withCompactStorage()
                .build();

        return CqlQuery.builder()
                .safeQueryFormat(query.getQuery() + " AND id = '%s'")
                .addArgs(SafeArg.of("cfId", getUuidForTable(tableRef)))
                .build();
    }

    private String wrapInQuotes(String string) {
        return "\"" + string + "\"";
    }

    private CompactionStrategy<?> getCompaction(boolean appendHeavyReadLight) {
        return appendHeavyReadLight
                ? SchemaBuilder.sizeTieredCompactionStrategy().withMinThreshold(4).withMaxThreshold(32)
                : SchemaBuilder.leveledCompactionStrategy();
    }

    private Integer getCompression(int blockSize) {
        return blockSize != 0 ? blockSize : AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
    }

    private UUID getUuidForTable(TableReference tableRef) {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(tableRef);
        String keyspace = config.getKeyspaceOrThrow();
        String fullTableNameForUuid = keyspace + "." + internalTableName;
        return UUID.nameUUIDFromBytes(fullTableNameForUuid.getBytes());
    }
}
