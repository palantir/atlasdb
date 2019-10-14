/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;

public class QueryCacheTest {

    private static final StatementPreparer STATEMENT_PREPARER =
            querySpec -> mock(PreparedStatement.class);
    private static final MetricsManager METRICS_MANAGER = MetricsManagers.createForTests();
    private static final String KEYSPACE = "foo";
    private static final TableReference TABLE_REFERENCE = tableReference("bar");

    private QueryCache cache;

    @Before
    public void setUp() {
        cache = QueryCache.create(STATEMENT_PREPARER, METRICS_MANAGER.getTaggedRegistry(), 100);
    }

    @Test
    public void testGetQuerySpecAllSame() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecTimestampIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 1))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecColumnIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 7, 3))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecRowIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, 4, 10, 3))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecKeyspaceNotIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement("baz", TABLE_REFERENCE, 10, 10, 3))
                .isNotEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecTableRefNotIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, 10, 10, 3);
        assertThat(prepareStatement(KEYSPACE, tableReference("baz"), 10, 10, 3))
                .isNotEqualTo(expectedPreparedStatement);
    }

    private PreparedStatement prepareStatement(
            String keyspace,
            TableReference tableReference,
            int row,
            int column,
            int timestamp) {
        return cache.prepare(getQuerySpec(keyspace, tableReference, row, column, timestamp));
    }

    private static TableReference tableReference(String baz) {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, baz);
    }

    private static GetQuerySpec getQuerySpec(
            String keyspace,
            TableReference tableReference,
            long rowValue,
            long columnValue,
            int timestamp) {
        return ImmutableGetQuerySpec.builder()
                .keySpace(keyspace)
                .tableReference(tableReference)
                .column(ByteBuffer.wrap(PtBytes.toBytes(rowValue)))
                .row(ByteBuffer.wrap(PtBytes.toBytes(columnValue)))
                .humanReadableTimestamp(timestamp)
                .executor(mock(Executor.class))
                .build();
    }
}
