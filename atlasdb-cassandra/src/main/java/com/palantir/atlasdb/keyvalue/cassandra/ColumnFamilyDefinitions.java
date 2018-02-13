/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.TriggerDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.exception.PalantirRuntimeException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

final class ColumnFamilyDefinitions {
    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueService.class); // did this on purpose

    private ColumnFamilyDefinitions() {
        // Utility class
    }

    /**
     *  Provides a default column family definition given raw metadata, which is generally obtained from the _metadata
     *  table.
     *
     *  Warning to developers: you must update CKVS.isMatchingCf if you update this method
     */
    @SuppressWarnings("CyclomaticComplexity")
    static CfDef getCfDef(String keyspace, TableReference tableRef, int gcGraceSeconds, byte[] rawMetadata) {
        Map<String, String> compressionOptions = Maps.newHashMap();
        CfDef cf = getStandardCfDef(keyspace, AbstractKeyValueService.internalTableName(tableRef));

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

        if (explicitCompressionBlockSizeKb != 0) {
            compressionOptions.put(
                    CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY,
                    CassandraConstants.DEFAULT_COMPRESSION_TYPE);
            compressionOptions.put(
                    CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY,
                    Integer.toString(explicitCompressionBlockSizeKb));
        } else {
            // We don't really need compression here nor anticipate it will garner us any gains
            // (which is why we're doing such a small chunk size), but this is how we can get "free" CRC checking.
            compressionOptions.put(
                    CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY,
                    CassandraConstants.DEFAULT_COMPRESSION_TYPE);
            compressionOptions.put(
                    CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY,
                    Integer.toString(AtlasDbConstants.MINIMUM_COMPRESSION_BLOCK_SIZE_KB));
        }

        if (negativeLookups) {
            falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_BLOOM_FILTER_FP_CHANCE;
        }

        if (appendHeavyAndReadLight) {
            cf.setCompaction_strategy(CassandraConstants.SIZE_TIERED_COMPACTION_STRATEGY);
            // clear out the now nonsensical "keep it at 80MB per sstable" option from LCS
            cf.setCompaction_strategy_optionsIsSet(false);
            if (!negativeLookups) {
                falsePositiveChance = CassandraConstants.DEFAULT_SIZE_TIERED_COMPACTION_BLOOM_FILTER_FP_CHANCE;
            } else {
                falsePositiveChance = CassandraConstants.NEGATIVE_LOOKUPS_SIZE_TIERED_BLOOM_FILTER_FP_CHANCE;
            }
        }

        switch (cachePriority) {
            case COLDEST:
                break;
            case COLD:
                break;
            case WARM:
                break;
            case HOT:
                break;
            case HOTTEST:
                cf.setPopulate_io_cache_on_flushIsSet(true);
                break;
            default:
                throw new PalantirRuntimeException("Unknown cache priority: " + cachePriority);
        }

        cf.setGc_grace_seconds(gcGraceSeconds);
        cf.setBloom_filter_fp_chance(falsePositiveChance);
        cf.setCompression_options(compressionOptions);
        return cf;
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
        cf.setCompression_options(Maps.<String, String>newHashMap());
        cf.setGc_grace_seconds(CassandraConstants.DEFAULT_GC_GRACE_SECONDS);

        // explicitly set fields to default values
        cf.setCaching("KEYS_ONLY");
        cf.setDclocal_read_repair_chance(0.1);
        cf.setTriggers(new ArrayList<TriggerDef>());
        cf.setCells_per_row_to_cache("0");
        cf.setMin_index_interval(128);
        cf.setMax_index_interval(2048);
        cf.setComment("");
        cf.setColumn_metadata(new ArrayList<ColumnDef>());
        cf.setMin_compaction_threshold(4);
        cf.setMax_compaction_threshold(32);
        cf.setKey_validation_class("org.apache.cassandra.db.marshal.BytesType");
        cf.setCompaction_strategy_options(new HashMap<String, String>());
        cf.setDefault_validation_class("org.apache.cassandra.db.marshal.BytesType");
        return cf;
    }

    // because unfortunately .equals takes into account if fields with defaults are populated or not
    // also because compression_options after serialization / deserialization comes back as blank
    // for the ones we set 4K chunk on... ?!
    @SuppressWarnings("CyclomaticComplexity")
    static boolean isMatchingCf(CfDef clientSide, CfDef clusterSide) {
        String tableName = clientSide.name;
        if (clientSide.compaction_strategy != clusterSide.compaction_strategy) {
            logMismatch("compaction strategy",
                    tableName,
                    clientSide.compaction_strategy,
                    clusterSide.compaction_strategy);
            return false;
        }
        if (clientSide.gc_grace_seconds != clusterSide.gc_grace_seconds) {
            logMismatch("gc_grace_seconds period",
                    tableName,
                    clientSide.gc_grace_seconds,
                    clusterSide.gc_grace_seconds);
            return false;
        }
        if (clientSide.bloom_filter_fp_chance != clusterSide.bloom_filter_fp_chance) {
            logMismatch("bloom filter false positive chance",
                    tableName,
                    clientSide.bloom_filter_fp_chance,
                    clusterSide.bloom_filter_fp_chance);
            return false;
        }
        if (!(clientSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY).equals(
                clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY)))) {
            logMismatch("compression chunk length",
                    tableName,
                    clientSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY),
                    clusterSide.compression_options.get(CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY));
            return false;
        }
        if (!Objects.equal(clientSide.compaction_strategy, clusterSide.compaction_strategy)) {
            // consider equal "com.whatever.LevelledCompactionStrategy" and "LevelledCompactionStrategy"
            if (clientSide.compaction_strategy != null
                    && clusterSide.compaction_strategy != null
                    && !(clientSide.compaction_strategy.endsWith(clusterSide.compaction_strategy)
                    || clusterSide.compaction_strategy.endsWith(clientSide.compaction_strategy))) {
                logMismatch("compaction strategy",
                        tableName,
                        clientSide.compaction_strategy,
                        clusterSide.compaction_strategy);
                return false;
            }
        }
        if (clientSide.isSetPopulate_io_cache_on_flush() != clusterSide.isSetPopulate_io_cache_on_flush()) {
            logMismatch("populate_io_cache_on_flush",
                    tableName,
                    clientSide.isSetPopulate_io_cache_on_flush(),
                    clusterSide.isSetPopulate_io_cache_on_flush());
            return false;
        }

        return true;
    }

    private static void logMismatch(String fieldName, String tableName,
            Object clientSideVersion, Object clusterSideVersion) {
        log.info("Found client/server disagreement on {} for table {}. (client = ({}), server = ({}))",
                SafeArg.of("disagreementType", fieldName),
                UnsafeArg.of("table", tableName),
                SafeArg.of("clientVersion", clientSideVersion),
                SafeArg.of("clusterVersion", clusterSideVersion));
    }
}
