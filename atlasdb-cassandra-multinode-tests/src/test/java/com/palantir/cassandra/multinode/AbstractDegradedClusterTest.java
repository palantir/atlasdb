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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices;
import com.palantir.common.base.RunnableCheckedException;
import com.palantir.common.exception.AtlasDbDependencyException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.thrift.TException;
import org.junit.Before;

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

    private String schemaAtStart;

    @Before
    public void recordSchemaVersion() throws TException {
        schemaAtStart = getUniqueReachableSchemaVersionOrThrow();
    }

    /**
     * Used in {@link NodesDownTestSetup#initializeKvsAndDegradeCluster(List, List)} using reflection to perform any
     * necessary initialization before degrading the cluster.
     *
     * @param kvs a dedicated instance of CKVs keyed to a specific namespace to be used with this test only
     */
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

    void assertThrowsAtlasDbDependencyExceptionAndDoesNotChangeCassandraSchema(RunnableCheckedException<?> task) {
        assertThatThrownBy(task::run).isInstanceOf(AtlasDbDependencyException.class);
        assertCassandraSchemaUnchanged();
    }

    void assertThrowsInsufficientConsistencyExceptionAndDoesNotChangeCassandraSchema(RunnableCheckedException<?> task) {
        assertThatThrownBy(task::run).isInstanceOf(InsufficientConsistencyException.class);
        assertCassandraSchemaUnchanged();
    }

    void assertCassandraSchemaChanged() {
        try {
            assertThat(getUniqueReachableSchemaVersionOrThrow()).isNotEqualTo(schemaAtStart);
        } catch (TException e) {
            fail("Encountered Thrift exception", e);
        }
    }

    private void assertCassandraSchemaUnchanged() {
        try {
            assertThat(getUniqueReachableSchemaVersionOrThrow()).isEqualTo(schemaAtStart);
        } catch (TException e) {
            fail("Encountered Thrift exception", e);
        }
    }

    private String getUniqueReachableSchemaVersionOrThrow() throws TException {
        Map<String, List<String>> schemaVersions = getTestKvs().getClientPool().runWithRetry(
                CassandraClient::describe_schema_versions);
        return Iterables.getOnlyElement(
                schemaVersions.keySet().stream()
                        .filter(schema -> !schema.equals(CassandraKeyValueServices.VERSION_UNREACHABLE))
                        .collect(Collectors.toList()));
    }
}
