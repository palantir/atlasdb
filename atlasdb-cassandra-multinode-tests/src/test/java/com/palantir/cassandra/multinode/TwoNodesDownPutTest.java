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
package com.palantir.cassandra.multinode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import java.util.Map;
import org.junit.Test;

public class TwoNodesDownPutTest extends AbstractDegradedClusterTest {
    private static final Cell EMPTY_CELL = Cell.create(PtBytes.toBytes("empty"), FIRST_COLUMN);
    private static final Cell NONEMPTY_CELL = Cell.create(PtBytes.toBytes("nonempty"), FIRST_COLUMN);

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.putUnlessExists(TEST_TABLE, ImmutableMap.of(NONEMPTY_CELL, CONTENTS));
    }

    @Test
    public void putThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().put(TEST_TABLE, ImmutableMap.of(CELL_1_1, CONTENTS), TIMESTAMP));
    }

    @Test
    public void putWithTimestampsThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().putWithTimestamps(TEST_TABLE, ImmutableMultimap.of(CELL_1_2, VALUE)));
    }

    @Test
    public void multiPutThrows() {
        Map<Cell, byte[]> entries = ImmutableMap.of(CELL_2_1, CONTENTS, CELL_2_2, CONTENTS);
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().multiPut(ImmutableMap.of(TEST_TABLE, entries), TIMESTAMP));
    }

    @Test
    public void putUnlessExistsThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().putUnlessExists(TEST_TABLE, ImmutableMap.of(EMPTY_CELL, CONTENTS)));
    }

    @Test
    public void putUnlessExistsThrowsAtlasDbDependencyExceptionOnExists() {
        byte[] newContents = PtBytes.toBytes("new_value");
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().putUnlessExists(TEST_TABLE, ImmutableMap.of(NONEMPTY_CELL, newContents)));
    }

    @Test
    public void addGarbageCollectionSentinelValuesThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(
                () -> getTestKvs().addGarbageCollectionSentinelValues(TEST_TABLE, ImmutableSet.of(CELL_2_2)));
    }
}
