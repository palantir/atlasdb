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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;

public abstract class AbstractDegradedClusterTest {
    static final TableReference TEST_TABLE = TableReference.createWithEmptyNamespace("test_table");
    static final byte[] FIRST_ROW = PtBytes.toBytes("row1");
    static final byte[] SECOND_ROW = PtBytes.toBytes("row2");
    static final byte[] FIRST_COLUMN = PtBytes.toBytes("col1");
    static final byte[] SECOND_COLUMN = PtBytes.toBytes("col2");
    static final Cell CELL_1_1 = Cell.create(FIRST_ROW, FIRST_COLUMN);
    static final Cell CELL_1_2 = Cell.create(FIRST_ROW, SECOND_COLUMN);
    static final Cell CELL_2_1 = Cell.create(SECOND_ROW, FIRST_COLUMN);
    static final Cell CELL_2_2 = Cell.create(SECOND_ROW, SECOND_COLUMN);
    static final byte[] CONTENTS = PtBytes.toBytes("default_value");
    static final long TIMESTAMP = 2L;
    static final Value VALUE = Value.create(CONTENTS, TIMESTAMP);

    private static final Map<Class<? extends AbstractDegradedClusterTest>, CassandraKeyValueService> testKvs =
            new HashMap<>();

    public void initialize(CassandraKeyValueService kvs) {
        testKvs.put(getClass(), kvs);
        testSetup(kvs);
    }

    abstract void testSetup(CassandraKeyValueService kvs);

    CassandraKeyValueService getTestKvs() {
        return testKvs.get(getClass());
    }

    static void closeAll() {
        testKvs.values().forEach(KeyValueService::close);
        testKvs.clear();
    }

    void assertKvsReturnsGenericMetadata(TableReference tableRef) {
        assertThat(getTestKvs().getMetadataForTable(tableRef)).isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }

    void assertKvsReturnsEmptyMetadata(TableReference tableRef) {
        assertThat(getTestKvs().getMetadataForTable(tableRef)).isEqualTo(AtlasDbConstants.GENERIC_TABLE_METADATA);
    }
}
