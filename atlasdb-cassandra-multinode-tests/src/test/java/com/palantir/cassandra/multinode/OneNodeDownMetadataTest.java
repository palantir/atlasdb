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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.exception.AtlasDbDependencyException;

public class OneNodeDownMetadataTest extends AbstractDegradedClusterTest {
    private static final byte[] metadata = new TableMetadata(new NameMetadataDescription(),
            new ColumnMetadataDescription(), ConflictHandler.IGNORE_ALL).persistToBytes();

    @Override
    void testSetup(CassandraKeyValueService kvs) {
        kvs.createTable(TEST_TABLE, AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void canGetMetadataForTable() {
        assertGenericMetadataForTestTable();
    }

    @Test
    public void canGetMetadataForAll() {
        Map<TableReference, byte[]> metadataMap = getTestKvs().getMetadataForTables();
        assertThat(getTestKvs().getMetadataForTables().get(TEST_TABLE))
                .isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
        assertThat(metadataMap.get(TEST_TABLE)).isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    @Test
    public void putMetadataForTableThrows() {
        assertThatThrownBy(() -> getTestKvs().putMetadataForTable(TEST_TABLE, metadata))
                .isInstanceOf(AtlasDbDependencyException.class);

        assertGenericMetadataForTestTable();
    }

    @Test
    public void putMetadataForTablesThrows() {
        assertThatThrownBy(() -> getTestKvs().putMetadataForTables(ImmutableMap.of(TEST_TABLE, metadata)))
                .isInstanceOf(AtlasDbDependencyException.class);

        assertGenericMetadataForTestTable();
    }

    private void assertGenericMetadataForTestTable() {
        assertThat(getTestKvs().getMetadataForTable(TEST_TABLE)).isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }
}
