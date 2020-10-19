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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import org.junit.Test;

public class OneNodeDownMetadataTest extends AbstractDegradedClusterTest {
    private static final TableReference TEST_TABLE_2 = TableReference.createWithEmptyNamespace("test_table_2");
    private static final TableReference TEST_TABLE_3 = TableReference.createWithEmptyNamespace("test_table_3");

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
        kvs.createTable(TEST_TABLE_2, AtlasDbConstants.EMPTY_TABLE_METADATA);
        kvs.createTable(TEST_TABLE_3, AtlasDbConstants.EMPTY_TABLE_METADATA);
    }

    @Test
    public void canGetMetadataForTable() {
        assertKvsReturnsGenericMetadata(TEST_TABLE);
    }

    @Test
    public void canGetMetadataForAll() {
        assertThat(getTestKvs().getMetadataForTables().get(TEST_TABLE))
                .isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void canPutMetadataForTable() {
        getTestKvs().putMetadataForTable(TEST_TABLE_2, AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertKvsReturnsGenericMetadata(TEST_TABLE_2);
        assertCassandraSchemaChanged();
    }

    @Test
    public void canPutMetadataForTables() {
        getTestKvs().putMetadataForTables(ImmutableMap.of(TEST_TABLE_3, AtlasDbConstants.GENERIC_TABLE_METADATA));
        assertKvsReturnsGenericMetadata(TEST_TABLE_3);
        assertCassandraSchemaChanged();
    }
}
