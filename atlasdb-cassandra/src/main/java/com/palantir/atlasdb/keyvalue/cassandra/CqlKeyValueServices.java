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

import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority.HOTTEST;

import org.apache.cassandra.thrift.Compression;
import org.apache.thrift.TException;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.SafeArg;

public final class CqlKeyValueServices {

    void createTableWithSettings(TableReference tableRef, byte[] rawMetadata, CassandraKeyValueServiceConfig config,
            CassandraClient client, CfIdTable cfIdTable) throws TException {
        StringBuilder queryBuilder = new StringBuilder();

        boolean negativeLookups = false;
        double falsePositiveChance = CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
        int explicitCompressionBlockSizeKb = 0;
        boolean appendHeavyAndReadLight = false;
        TableMetadataPersistence.CachePriority cachePriority = TableMetadataPersistence.CachePriority.WARM;

        if (!CassandraKeyValueServices.isEmptyOrInvalidMetadata(rawMetadata)) {
            TableMetadata tableMetadata = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata);
            negativeLookups = tableMetadata.hasNegativeLookups();
            explicitCompressionBlockSizeKb = tableMetadata.getExplicitCompressionBlockSizeKB();
            appendHeavyAndReadLight = tableMetadata.isAppendHeavyAndReadLight();
            cachePriority = tableMetadata.getCachePriority();
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        }

        String compactionStrategy = CassandraConstants.LEVELED_COMPACTION_STRATEGY;
        if (appendHeavyAndReadLight) {
            compactionStrategy = CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY;
            if (!negativeLookups) {
                falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
            } else {
                falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE;
            }
        }

        int chunkLength = AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB;
        if (explicitCompressionBlockSizeKb != 0) {
            chunkLength = explicitCompressionBlockSizeKb;
        }

        CqlFieldNameProvider provider = new CqlFieldNameProvider();


        queryBuilder.append("CREATE TABLE IF NOT EXISTS "
                + "\"%s\".\"%s\""
                + " ( "
                + provider.row() + " blob, "
                + provider.column() + " blob, "
                + provider.timestamp() + " bigint, "
                + provider.value() + " blob, "
                + "PRIMARY KEY ("
                + provider.row() + ", "
                + provider.column() + ", "
                + provider.timestamp() + ")) "
                + "WITH COMPACT STORAGE ");
        queryBuilder.append("AND caching = 'KEYS_ONLY' ");
        queryBuilder.append("AND speculative_retry = 'NONE' ");

        queryBuilder.append("AND " + "bloom_filter_fp_chance = " + falsePositiveChance + " ");
        queryBuilder.append("AND " + "populate_io_cache_on_flush = " + cachePriority.equals(HOTTEST) + " ");
        queryBuilder.append("AND gc_grace_seconds = " + config.gcGraceSeconds() + " ");
        queryBuilder.append("AND compaction = { 'class': '" + compactionStrategy + "'} ");
        queryBuilder.append("AND compression = {'chunk_length_kb': '" + chunkLength + "', "
                + "'sstable_compression': '" + CassandraConstants.DEFAULT_COMPRESSION_TYPE + "'} ");
        queryBuilder.append("AND CLUSTERING ORDER BY ("
                + provider.column() + " ASC, "
                + provider.timestamp() + " ASC) ");

        queryBuilder.append("AND id = '%s'");
        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat(queryBuilder.toString())
                .addArgs(
                        SafeArg.of("keyspace", config.getKeyspaceOrThrow()),
                        SafeArg.of("internalTableName", CassandraKeyValueServiceImpl.internalTableName(tableRef)),
                        SafeArg.of("cfId", cfIdTable.getCfIdForTable(tableRef)))
                .build();

        client.execute_cql3_query(query, Compression.NONE, org.apache.cassandra.thrift.ConsistencyLevel.QUORUM);
        CassandraKeyValueServices.waitForSchemaVersions(config, client, "test");
    }
}
