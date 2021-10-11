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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.cassandra.thrift.CfDef;

final class ColumnFamilyDefinitions {
    private static final SafeLogger log = SafeLoggerFactory.get(ColumnFamilyDefinitions.class);

    private ColumnFamilyDefinitions() {
        // Utility class
    }

    /**
     *  Provides a default column family definition given raw metadata, which is generally obtained from the _metadata
     *  table.
     *
     *  Warning to developers: you must update CKVS.isMatchingCf if you update this method
     */
    static CfDef getCfDef(String keyspace, TableReference tableRef, int gcGraceSeconds, byte[] rawMetadata) {
        CfDef cf = getStandardCfDef(keyspace, AbstractKeyValueService.internalTableName(tableRef));

        Optional<TableMetadata> tableMetadata = CassandraKeyValueServices.isEmptyOrInvalidMetadata(rawMetadata)
                ? Optional.empty()
                : Optional.of(TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(rawMetadata));

        Map<String, String> compressionOptions = getCompressionOptions(tableMetadata);
        cf.setCompression_options(compressionOptions);

        boolean appendHeavyAndReadLight =
                tableMetadata.map(TableMetadata::isAppendHeavyAndReadLight).orElse(false);
        if (appendHeavyAndReadLight) {
            cf.setCompaction_strategy(CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY);
            cf.setCompaction_strategy_optionsIsSet(false);
        }

        cf.setGc_grace_seconds(gcGraceSeconds);

        double bloomFilterFpChance = tableMetadata
                .map(CassandraTableOptions::bloomFilterFpChance)
                .orElse(CassandraConstants.DEFAULT_LEVELED_COMPACTION_BLOOM_FILTER_FP_CHANCE);
        cf.setBloom_filter_fp_chance(bloomFilterFpChance);

        int minIndexInterval = tableMetadata
                .map(CassandraTableOptions::minIndexInterval)
                .orElse(CassandraConstants.DEFAULT_MIN_INDEX_INTERVAL);
        cf.setMin_index_interval(minIndexInterval);

        int maxIndexInterval = tableMetadata
                .map(CassandraTableOptions::maxIndexInterval)
                .orElse(CassandraConstants.DEFAULT_MAX_INDEX_INTERVAL);
        cf.setMax_index_interval(maxIndexInterval);
        return cf;
    }

    private static Map<String, String> getCompressionOptions(Optional<TableMetadata> tableMetadata) {
        int explicitCompressionBlockSizeKb = tableMetadata
                .map(TableMetadata::getExplicitCompressionBlockSizeKB)
                .orElse(0);
        int actualCompressionBlockSizeKb = explicitCompressionBlockSizeKb == 0
                ? AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB
                : explicitCompressionBlockSizeKb;
        return ImmutableMap.<String, String>builder()
                .put(CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY, CassandraConstants.DEFAULT_COMPRESSION_TYPE)
                .put(
                        CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY,
                        Integer.toString(actualCompressionBlockSizeKb))
                .build();
    }

    /**
     *  Provides a basic column family definition. This is a subset of #getCfDef, and does not
     *  include compression options, but also does not require raw metadata to be passed in.
     *
     *  Warning to developers: you must update CKVS.isMatchingCf if you update this method
     */
    static CfDef getStandardCfDef(String keyspace, String internalTableName) {
        CfDef cf = new CfDef(keyspace, internalTableName);
        cf.setComparator_type("CompositeType(BytesType,LongType)");
        cf.setCompaction_strategy(CassandraConstants.LEVELED_COMPACTION_STRATEGY);
        cf.setCompression_options(new HashMap<String, String>());
        cf.setGc_grace_seconds(CassandraConstants.DEFAULT_GC_GRACE_SECONDS);

        // explicitly set fields to default values
        cf.setCaching("KEYS_ONLY");
        cf.setDclocal_read_repair_chance(0.0);
        cf.setTriggers(ImmutableList.of());
        cf.setCells_per_row_to_cache("0");
        cf.setMin_index_interval(CassandraConstants.DEFAULT_MIN_INDEX_INTERVAL);
        cf.setMax_index_interval(CassandraConstants.DEFAULT_MAX_INDEX_INTERVAL);
        cf.setComment("");
        cf.setColumn_metadata(ImmutableList.of());
        cf.setMin_compaction_threshold(4);
        cf.setMax_compaction_threshold(32);
        cf.setKey_validation_class("org.apache.cassandra.db.marshal.BytesType");
        cf.setCompaction_strategy_options(ImmutableMap.of());
        cf.setDefault_validation_class("org.apache.cassandra.db.marshal.BytesType");
        return cf;
    }

    // because unfortunately .equals takes into account if fields with defaults are populated or not
    // also because compression_options after serialization / deserialization comes back as blank
    // for the ones we set 4K chunk on... ?!
    @SuppressWarnings("CyclomaticComplexity")
    static boolean isMatchingCf(CfDef clientSide, CfDef clusterSide) {
        String tableName = clientSide.name;
        if (!equalsIgnoringClasspath(clientSide.compaction_strategy, clusterSide.compaction_strategy)) {
            logMismatch(
                    "compaction strategy", tableName, clientSide.compaction_strategy, clusterSide.compaction_strategy);
            return false;
        }
        if (!optionsMapsFunctionallyEqual(
                clientSide.compaction_strategy_options, clusterSide.compaction_strategy_options)) {
            logMismatch(
                    "compaction strategy options",
                    tableName,
                    clientSide.compaction_strategy_options,
                    clusterSide.compaction_strategy_options);
            return false;
        }
        if (clientSide.gc_grace_seconds != clusterSide.gc_grace_seconds) {
            logMismatch(
                    "gc_grace_seconds period", tableName, clientSide.gc_grace_seconds, clusterSide.gc_grace_seconds);
            return false;
        }
        if (clientSide.bloom_filter_fp_chance != clusterSide.bloom_filter_fp_chance) {
            logMismatch(
                    "bloom filter false positive chance",
                    tableName,
                    clientSide.bloom_filter_fp_chance,
                    clusterSide.bloom_filter_fp_chance);
            return false;
        }
        if (!clientSide
                .compression_options
                .get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY)
                .equals(clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY))) {
            logMismatch(
                    "compression chunk length",
                    tableName,
                    clientSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY),
                    clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY));
            return false;
        }
        if (!Objects.equals(clientSide.compaction_strategy, clusterSide.compaction_strategy)) {
            // consider equal "com.whatever.LevelledCompactionStrategy" and "LevelledCompactionStrategy"
            if (clientSide.compaction_strategy != null
                    && clusterSide.compaction_strategy != null
                    && !(clientSide.compaction_strategy.endsWith(clusterSide.compaction_strategy)
                            || clusterSide.compaction_strategy.endsWith(clientSide.compaction_strategy))) {
                logMismatch(
                        "compaction strategy",
                        tableName,
                        clientSide.compaction_strategy,
                        clusterSide.compaction_strategy);
                return false;
            }
        }
        if (clientSide.min_index_interval != clusterSide.min_index_interval) {
            logMismatch("min index interval", tableName, clientSide.min_index_interval, clusterSide.min_index_interval);
            return false;
        }
        if (clientSide.max_index_interval != clusterSide.max_index_interval) {
            logMismatch("max index interval", tableName, clientSide.max_index_interval, clusterSide.max_index_interval);
            return false;
        }

        return true;
    }

    private static void logMismatch(
            String fieldName, String tableName, Object clientSideVersion, Object clusterSideVersion) {
        log.info(
                "Found client/server disagreement on {} for table {}. (client = ({}), server = ({}))",
                SafeArg.of("disagreementType", fieldName),
                LoggingArgs.tableRef(TableReference.createUnsafe(tableName)),
                SafeArg.of("clientVersion", clientSideVersion),
                SafeArg.of("clusterVersion", clusterSideVersion));
    }

    static boolean equalsIgnoringClasspath(String class1, String class2) {
        if (Objects.equals(class1, class2)) {
            return true;
        }
        if (class1 == null || class2 == null) {
            return false;
        }
        if (getLastElementOfClasspath(class1).equals(getLastElementOfClasspath(class2))) {
            return true;
        }
        return false;
    }

    private static String getLastElementOfClasspath(String classpath) {
        if (classpath.contains(".")) {
            List<String> periodDelimitedClasspath = Splitter.on(".").splitToList(classpath);
            return periodDelimitedClasspath.get(periodDelimitedClasspath.size() - 1);
        } else {
            return classpath;
        }
    }

    private static boolean optionsMapsFunctionallyEqual(Map<String, String> client, Map<String, String> cluster) {
        if (Objects.equals(client, cluster)) {
            return true;
        }

        // consider null and empty map equivalent
        if ((client == null && cluster.isEmpty()) || (cluster == null && client.isEmpty())) {
            return true;
        }

        return false;
    }
}
