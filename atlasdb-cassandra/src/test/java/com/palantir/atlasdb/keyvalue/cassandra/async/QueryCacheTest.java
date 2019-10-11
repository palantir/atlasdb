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

    private static final StatementPreparer MOCK_RETURNING_STATEMENT_PREPARER =
            querySpec -> mock(PreparedStatement.class);
    private static final MetricsManager METRICS_MANAGER = MetricsManagers.createForTests();
    private static final String KEYSPACE = "foo";
    private static final TableReference TABLE_REFERENCE =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "bar");
    private static final Executor TESTING_EXECUTOR = mock(Executor.class);

    private QueryCache cache;
    private PreparedStatement expectedPreparedStatement;

    @Before
    public void setUp() {
        cache = QueryCache.create(
                MOCK_RETURNING_STATEMENT_PREPARER,
                METRICS_MANAGER.getTaggedRegistry(),
                100);

        expectedPreparedStatement = cache.prepare(
                getQuerySpecWithRandomData(
                        KEYSPACE,
                        TABLE_REFERENCE,
                        PtBytes.toBytes(10),
                        PtBytes.toBytes(10),
                        3));
    }

    @Test
    public void testGetQuerySpecAllSame() {
        assertThat(expectedPreparedStatement)
                .isEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TABLE_REFERENCE,
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(10),
                                3)));
    }

    @Test
    public void testGetQuerySpecTimestampIgnored() {
        assertThat(expectedPreparedStatement)
                .isEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TABLE_REFERENCE,
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(10),
                                1)));
    }

    @Test
    public void testGetQuerySpecColumnIgnored() {
        assertThat(expectedPreparedStatement)
                .isEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TABLE_REFERENCE,
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(7),
                                3)));
    }

    @Test
    public void testGetQuerySpecRowIgnored() {
        assertThat(expectedPreparedStatement)
                .isEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TABLE_REFERENCE,
                                PtBytes.toBytes(4),
                                PtBytes.toBytes(10),
                                3)));
    }

    @Test
    public void testGetQuerySpecKeyspaceNotIgnored() {
        assertThat(expectedPreparedStatement)
                .isNotEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                "baz",
                                TABLE_REFERENCE,
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(10),
                                3)));
    }

    @Test
    public void testGetQuerySpecTableRefNotIgnored() {
        assertThat(expectedPreparedStatement)
                .isNotEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TableReference.create(Namespace.DEFAULT_NAMESPACE, "baz"),
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(10),
                                3)));
    }

    @Test
    public void testGetQuerySpecExecutorNotIgnored() {
        assertThat(expectedPreparedStatement)
                .isNotEqualTo(
                        cache.prepare(getQuerySpecWithRandomData(
                                KEYSPACE,
                                TABLE_REFERENCE,
                                PtBytes.toBytes(10),
                                PtBytes.toBytes(10),
                                3,
                                mock(Executor.class))));
    }

    private static GetQuerySpec getQuerySpecWithRandomData(
            String keyspace,
            TableReference tableReference,
            byte[] rowValue,
            byte[] columnValue,
            int timestamp,
            Executor executor) {
        return ImmutableGetQuerySpec.builder()
                .keySpace(keyspace)
                .tableReference(tableReference)
                .column(ByteBuffer.wrap(rowValue))
                .row(ByteBuffer.wrap(columnValue))
                .humanReadableTimestamp(timestamp)
                .executor(executor)
                .build();
    }

    private static GetQuerySpec getQuerySpecWithRandomData(
            String keyspace,
            TableReference tableReference,
            byte[] rowValue,
            byte[] columnValue,
            int timestamp) {
        return getQuerySpecWithRandomData(
                keyspace,
                tableReference,
                rowValue,
                columnValue,
                timestamp,
                TESTING_EXECUTOR);
    }
}
