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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.thrift.CfDef;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraCompactionConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCompactionConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ColumnFamilyDefinitionsTest {
    private static final int FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60;
    private static final byte[] TABLE_METADATA_WITH_MANY_NON_DEFAULT_FEATURES =
            new TableMetadata(
                    new NameMetadataDescription(),
                    new ColumnMetadataDescription(),
                    ConflictHandler.RETRY_ON_WRITE_WRITE,
                    TableMetadataPersistence.CachePriority.WARM,
                    true,
                    64,
                    true,
                    TableMetadataPersistence.SweepStrategy.THOROUGH,
                    true).persistToBytes();

    // Tests may fail if the non-default-feature table metadata is changed to not be append heavy and read light.
    private static final byte[] APPEND_HEAVY_READ_LIGHT_TABLE_METADATA = TABLE_METADATA_WITH_MANY_NON_DEFAULT_FEATURES;

    @Test
    public void compactionStrategiesShouldMatchWithOrWithoutPackageName() {
        CfDef standard = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                CassandraConstants.DEFAULT_GC_GRACE_SECONDS, new byte[0]);

        CfDef fullyQualified = standard.setCompaction_strategy("com.palantir.AwesomeCompactionStrategy");
        CfDef onlyClassName = standard.deepCopy().setCompaction_strategy("AwesomeCompactionStrategy");

        assertTrue(
                String.format("Compaction strategies %s and %s should match",
                        fullyQualified.compaction_strategy,
                        onlyClassName.compaction_strategy),
                ColumnFamilyDefinitions.isMatchingCf(fullyQualified, onlyClassName));
    }


    @Test
    public void cfDefWithDifferingGcGraceSecondsValuesShouldNotMatch() {
        CfDef clientSideTable = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                CassandraConstants.DEFAULT_GC_GRACE_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA);
        CfDef clusterSideTable = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertFalse("ColumnDefinitions with different gc_grace_seconds should not match",
                ColumnFamilyDefinitions.isMatchingCf(clientSideTable, clusterSideTable));
    }

    @Test
    public void nonDefaultFeaturesCorrectlyCompared() {
        CfDef cf1 = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                TABLE_METADATA_WITH_MANY_NON_DEFAULT_FEATURES);

        CfDef cf2 = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                TABLE_METADATA_WITH_MANY_NON_DEFAULT_FEATURES);

        assertTrue("identical CFs should equal each other", ColumnFamilyDefinitions.isMatchingCf(cf1, cf2));
    }

    @Test
    public void identicalCfsAreEqual() {
        CfDef cf1 = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        CfDef cf2 = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertTrue("identical CFs should equal each other", ColumnFamilyDefinitions.isMatchingCf(cf1, cf2));
    }

    @Test
    public void cfsWithDifferentCompactionStrategyParametersShouldNotMatch() {
        CfDef cf1 = getGenericCfDef();
        CfDef cf2 = getGenericCfDef();

        cf1.putToCompaction_strategy_options("tombstone_threshold", "0.05");
        cf2.putToCompaction_strategy_options("tombstone_threshold", "0.1");

        assertFalse("ColumnDefinitions with different tombstone_threshold should not match",
                ColumnFamilyDefinitions.isMatchingCf(cf1, cf2));
    }

    @Test
    public void cfsWithDefinedAndUndefinedCompactionStrategyParametersShouldNotMatch() {
        CfDef cf1 = getGenericCfDef();
        CfDef cf2 = getGenericCfDef();

        cf1.putToCompaction_strategy_options("tombstone_threshold", "0.05");

        assertFalse("cf2 shouldn't have a tombstone_threshold set",
                cf2.compaction_strategy_options.containsKey("tombstone_threshold"));
        assertFalse("ColumnDefinitions with different tombstone_threshold should not match",
                ColumnFamilyDefinitions.isMatchingCf(cf1, cf2));
    }

    @Test
    public void cfsWithAppendHeavyAndReadLightShouldHaveSizeTieredCompactionStrategy() {
        CfDef appendHeavy = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                APPEND_HEAVY_READ_LIGHT_TABLE_METADATA);

        assertTrue("ColumnDefinitions for an append-heavy-read-light table should have size tiered compaction",
                appendHeavy.compaction_strategy.contains("SizeTiered"));
    }

    @Test
    public void cfsForAppendHeavyReadLightStreamStoreValueTablesShouldAcceptCustomThreshold() {
        CfDef valueTable = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("atlas.blob_stream_value"),
                FOUR_DAYS_IN_SECONDS,
                APPEND_HEAVY_READ_LIGHT_TABLE_METADATA,
                compactionConfigWithThreshold(0.01));

        assertTrue("Compaction configuration should be set for an append-heavy-read-light stream store value table",
                valueTable.compaction_strategy_options.get("tombstone_threshold").equals(String.valueOf(0.01)));
    }

    @Test
    public void cfsForOtherStreamStoreTablesShouldNotFollowCustomThreshold() {
        CfDef hashAidxTable = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("atlas.blob_stream_hash_aidx"),
                FOUR_DAYS_IN_SECONDS,
                APPEND_HEAVY_READ_LIGHT_TABLE_METADATA,
                compactionConfigWithThreshold(0.01));

        assertFalse("Compaction configuration should not be set for stream tables that aren't the value table",
                hashAidxTable.isSetCompaction_strategy_options());
    }

    @Test
    public void cfsForRegularStreamStoreValueTablesShouldNotFollowCustomThreshold() {
        CfDef valueTable = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("atlas.blob_stream_value"),
                FOUR_DAYS_IN_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA,
                compactionConfigWithThreshold(0.01));

        assertFalse("Compaction configuration should not be set for a regular stream store value table",
                valueTable.compaction_strategy_options.containsKey("tombstone_threshold"));

    }

    @Test
    public void cfsForOtherAppendHeavyReadLightTablesShouldNotFollowCustomThreshold() {
        CfDef testCf = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("atlas.test_test_test"),
                FOUR_DAYS_IN_SECONDS,
                APPEND_HEAVY_READ_LIGHT_TABLE_METADATA,
                compactionConfigWithThreshold(0.01));

        assertFalse("Compaction configuration should not be set for append-heavy-read-light tables that aren't the"
                        + " value table",
                testCf.isSetCompaction_strategy_options());
    }

    private CfDef getGenericCfDef() {
        return ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                FOUR_DAYS_IN_SECONDS,
                AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    private CassandraCompactionConfig compactionConfigWithThreshold(double threshold) {
        return ImmutableCassandraCompactionConfig.builder()
                .appendHeavyReadLightStreamStoreTombstoneThreshold(threshold)
                .build();
    }
}
