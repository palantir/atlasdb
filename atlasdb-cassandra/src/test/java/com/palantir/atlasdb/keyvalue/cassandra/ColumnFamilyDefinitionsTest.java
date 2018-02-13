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
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class ColumnFamilyDefinitionsTest {
    private static final int FOUR_DAYS_IN_SECONDS = 4 * 24 * 60 * 60;

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
}
