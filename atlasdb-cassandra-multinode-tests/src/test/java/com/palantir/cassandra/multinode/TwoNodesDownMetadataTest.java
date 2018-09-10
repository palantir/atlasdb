/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.cassandra.multinode;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;

public class TwoNodesDownMetadataTest extends AbstractDegradedClusterTest {

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void getMetadataForTableThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().getMetadataForTable(TEST_TABLE));
    }

    @Test
    public void getMetadataForAllThrows() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().getMetadataForTables());
    }

    @Test
    public void putMetadataForTableThrowsAndDoesNotChangeCassandraSchema() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().putMetadataForTable(TEST_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA));
    }

    @Test
    public void putMetadataForTablesThrowsAndDoesNotChangeCassandraSchema() {
        assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(() ->
                getTestKvs().putMetadataForTables(ImmutableMap.of(TEST_TABLE, AtlasDbConstants.EMPTY_TABLE_METADATA)));
    }
}
