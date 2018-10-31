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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.Compression;
import org.apache.thrift.TException;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;

class CassandraTableCreator {
    private static final String ROW = "key";
    private static final String COLUMN = "column1";
    private static final String TIMESTAMP = "column2";
    private static final String VALUE = "value";

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
                    createTable(entry.getKey(), entry.getValue(), client);
                }
                CassandraKeyValueServices.waitForSchemaVersions(config, client, "after adding the column family for "
                        + "tables " + tableRefToMetadata.keySet() + " in a call to create tables");
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void createTable(TableReference tableRef, byte[] metadata, CassandraClient client) throws TException {
        CqlQuery query = constructQuery(tableRef, metadata);
        client.execute_cql3_query(query, Compression.NONE, org.apache.cassandra.thrift.ConsistencyLevel.QUORUM);
    }

    private CqlQuery constructQuery(TableReference tableRef, byte[] metadata) {
        StringBuilder queryBuilder = new StringBuilder();

        TableMetadata tableMetadata = CassandraKeyValueServices.getMetadataOrDefaultToGeneric(metadata);
        boolean appendHeavyReadLight = tableMetadata.isAppendHeavyAndReadLight();

        Map<String, String> settings = new HashMap<>();
        settings.put("bloom_filter_fp_chance", falsePositive(tableMetadata.hasNegativeLookups(), appendHeavyReadLight));
        settings.put("caching", "'KEYS_ONLY'");
        settings.put("compaction", getCompaction(appendHeavyReadLight));
        settings.put("compression", getCompression(tableMetadata.getExplicitCompressionBlockSizeKB()));
        settings.put("gc_grace_seconds", Integer.toString(config.gcGraceSeconds()));
        settings.put("id", "'%s'");
        settings.put("populate_io_cache_on_flush", cachePriorityIsHottest(tableMetadata));
        settings.put("speculative_retry", "'NONE'");

        queryBuilder.append("CREATE TABLE IF NOT EXISTS \"%s\".\"%s\""
                + " ( " + ROW + " blob, " + COLUMN + " blob, " + TIMESTAMP + " bigint, " + VALUE + " blob, "
                + "PRIMARY KEY (" + ROW + ", " + COLUMN + ", " + TIMESTAMP + ")) "
                + "WITH COMPACT STORAGE ");

        settings.forEach((option, value) -> queryBuilder.append("AND " + option + " = " + value + " "));

        queryBuilder.append("AND CLUSTERING ORDER BY (" + COLUMN + " ASC, " + TIMESTAMP + " ASC) ");

        return CqlQuery.builder()
                .safeQueryFormat(queryBuilder.toString())
                .addArgs(
                        SafeArg.of("keyspace", config.getKeyspaceOrThrow()),
                        SafeArg.of("internalTableName", CassandraKeyValueServiceImpl.internalTableName(tableRef)),
                        SafeArg.of("cfId", getUuidForTable(tableRef)))
                .build();
    }

    private String falsePositive(boolean negativeLookups, boolean appendHeavyAndReadLight) {
        double result;
        if (appendHeavyAndReadLight) {
            result = negativeLookups ? CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE
                    : CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        } else {
            result = negativeLookups ? CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE
                    : CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        }
        return Double.toString(result);
    }

    private String getCompaction(boolean appendHeavyReadLight) {
        String compactionStrategy = appendHeavyReadLight ? CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY
                : CassandraConstants.LEVELED_COMPACTION_STRATEGY;
        return "{ 'class': '" + compactionStrategy + "'}";
    }

    private String getCompression(int explicitCompressionBlockSizeKb) {
        int chunkLength = explicitCompressionBlockSizeKb != 0 ? explicitCompressionBlockSizeKb
                : AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        return "{'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'}";
    }

    private String cachePriorityIsHottest(TableMetadata tableMetadata) {
        return Boolean.toString(tableMetadata.getCachePriority()
                .equals(TableMetadataPersistence.CachePriority.HOTTEST));
    }

    private UUID getUuidForTable(TableReference tableRef) {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(tableRef);
        String keyspace = config.getKeyspaceOrThrow();
        String fullTableNameForUuid = keyspace + "." + internalTableName;
        return UUID.nameUUIDFromBytes(fullTableNameForUuid.getBytes());
    }
}
