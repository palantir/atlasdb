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
package com.palantir.cassandra.multinode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.TableMetadata;

public class OneNodeDownMetadataTest {
    private static final TableMetadata GENERIC = TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(
            AtlasDbConstants.GENERIC_TABLE_METADATA);

    @Test
    public void canGetMetadataForTable() {
        assertContainsGenericMetadata(OneNodeDownTestSuite.TEST_TABLE);
    }

    @Test
    public void canGetMetadataForAll() {
        Map<TableReference, byte[]> metadataMap = OneNodeDownTestSuite.kvs.getMetadataForTables();
        assertEquals(GENERIC,
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(metadataMap.get(OneNodeDownTestSuite.TEST_TABLE)));
    }

    @Test
    public void canPutMetadataForTable() {
        assertMetadataEmpty(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA);
        OneNodeDownTestSuite.kvs.putMetadataForTable(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA,
                AtlasDbConstants.GENERIC_TABLE_METADATA);

        assertContainsGenericMetadata(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA);
    }

    @Test
    public void canPutMetadataForTables() {
        assertMetadataEmpty(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA2);
        OneNodeDownTestSuite.kvs.putMetadataForTables(ImmutableMap.of(
                OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA2, AtlasDbConstants.GENERIC_TABLE_METADATA));

        assertContainsGenericMetadata(OneNodeDownTestSuite.TEST_TABLE_FOR_METADATA2);
    }

    static void assertContainsGenericMetadata(TableReference tableRef) {
        assertEquals(GENERIC,
                TableMetadata.BYTES_HYDRATOR.hydrateFromBytes(OneNodeDownTestSuite.kvs.getMetadataForTable(tableRef)));
    }

    static void assertMetadataEmpty(TableReference table) {
        assertArrayEquals(OneNodeDownTestSuite.kvs.getMetadataForTable(table), AtlasDbConstants.EMPTY_TABLE_METADATA);
    }
}
