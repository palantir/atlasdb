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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.table.description.TableMetadata;
import org.apache.cassandra.thrift.CfDef;
import org.junit.Test;

public class ColumnFamilyDefinitionsTest {
    private static final int FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60;
    private static final byte[] TABLE_METADATA_WITH_MANY_NON_DEFAULT_FEATURES = TableMetadata.builder()
            .rangeScanAllowed(true)
            .explicitCompressionBlockSizeKB(64)
            .negativeLookups(true)
            .sweepStrategy(TableMetadataPersistence.SweepStrategy.THOROUGH)
            .appendHeavyAndReadLight(true)
            .denselyAccessedWideRows(true)
            .build()
            .persistToBytes();
    private static final ImmutableMap<String, String> DEFAULT_COMPRESSION = ImmutableMap.of(
            CassandraConstants.CFDEF_COMPRESSION_TYPE_KEY,
            CassandraConstants.DEFAULT_COMPRESSION_TYPE,
            CassandraConstants.CFDEF_COMPRESSION_CHUNK_LENGTH_KEY,
            String.valueOf(4));

    @Test
    public void compactionStrategiesShouldMatchWithOrWithoutPackageName() {
        CfDef standard = ColumnFamilyDefinitions.getCfDef(
                "test_keyspace",
                TableReference.fromString("test_table"),
                CassandraConstants.DEFAULT_GC_GRACE_SECONDS,
                new byte[0]);

        CfDef fullyQualified = standard.setCompaction_strategy("com.palantir.AwesomeCompactionStrategy");
        CfDef onlyClassName = standard.deepCopy().setCompaction_strategy("AwesomeCompactionStrategy");

        assertThat(ColumnFamilyDefinitions.isMatchingCf(fullyQualified, onlyClassName))
                .describedAs(
                        "Compaction strategies %s and %s should match",
                        fullyQualified.compaction_strategy, onlyClassName.compaction_strategy)
                .isTrue();
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

        assertThat(ColumnFamilyDefinitions.isMatchingCf(clientSideTable, clusterSideTable))
                .describedAs("ColumnFamilyDefinitions with different gc_grace_seconds should not match")
                .isFalse();
    }

    @Test
    public void cfDefWithDenselyAccessedWideRowsShouldDifferFromOneWithout() {
        CfDef clientSideTable = ColumnFamilyDefinitions.getCfDef(
                "test",
                TableReference.fromString("cf_def"),
                CassandraConstants.DEFAULT_GC_GRACE_SECONDS,
                TableMetadata.builder().build().persistToBytes());
        CfDef clusterSideTable = ColumnFamilyDefinitions.getCfDef(
                "test",
                TableReference.fromString("cf_def"),
                CassandraConstants.DEFAULT_GC_GRACE_SECONDS,
                TableMetadata.builder().denselyAccessedWideRows(true).build().persistToBytes());

        assertThat(ColumnFamilyDefinitions.isMatchingCf(clientSideTable, clusterSideTable))
                .describedAs("denselyAccessedWideRows should be reflected in comparisons of ColumnFamilyDefinitions")
                .isFalse();
    }

    @Test
    public void cfDefWithDifferingMinIndexIntervalValuesShouldNotMatch() {
        CfDef clientSideTable = ColumnFamilyDefinitions.getStandardCfDef("test", "cf_def")
                .setCompression_options(DEFAULT_COMPRESSION)
                .setMin_index_interval(512);
        CfDef clusterSideTable = ColumnFamilyDefinitions.getStandardCfDef("test", "cf_def")
                .setCompression_options(DEFAULT_COMPRESSION)
                .setMin_index_interval(128);

        assertThat(ColumnFamilyDefinitions.isMatchingCf(clientSideTable, clusterSideTable))
                .describedAs("ColumnFamilyDefinitions with different min_index_interval should not match")
                .isFalse();
    }

    @Test
    public void cfDefWithDifferingMaxIndexIntervalValuesShouldNotMatch() {
        CfDef clientSideTable = ColumnFamilyDefinitions.getStandardCfDef("test", "cf_def")
                .setCompression_options(DEFAULT_COMPRESSION)
                .setMax_index_interval(512);
        CfDef clusterSideTable = ColumnFamilyDefinitions.getStandardCfDef("test", "cf_def")
                .setCompression_options(DEFAULT_COMPRESSION)
                .setMax_index_interval(128);

        assertThat(ColumnFamilyDefinitions.isMatchingCf(clientSideTable, clusterSideTable))
                .describedAs("ColumnFamilyDefinitions with different min_index_interval should not match")
                .isFalse();
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

        assertThat(ColumnFamilyDefinitions.isMatchingCf(cf1, cf2))
                .describedAs("identical CFs should equal each other")
                .isTrue();
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

        assertThat(ColumnFamilyDefinitions.isMatchingCf(cf1, cf2))
                .describedAs("identical CFs should equal each other")
                .isTrue();
    }

    @Test
    public void equalsIgnoringClasspath() {
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath(null, null)).isTrue();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("", "")).isTrue();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("a", "a")).isTrue();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("a.b", "a.b"))
                .isTrue();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("a.b", "b.b"))
                .isTrue();

        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath(null, "")).isFalse();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("", null)).isFalse();

        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("a", "b")).isFalse();
        assertThat(ColumnFamilyDefinitions.equalsIgnoringClasspath("a.b", "a.c"))
                .isFalse();
    }
}
