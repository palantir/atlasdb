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

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class QueryCacheTest {

    public static final ConsistencyLevel READ_CONSISTENCY = ConsistencyLevel.ALL;
    private static Integer counter = 0;
    private static final StatementPreparer ALWAYS_INCREASING_ENTRY_CREATOR = querySpec -> mock(PreparedStatement.class);

    private static final MetricsManager METRICS_MANAGER = MetricsManagers.createForTests();
    private static final TaggedMetricRegistry TAGGED_METRIC_REGISTRY = METRICS_MANAGER.getTaggedRegistry();

    private static final String KEYSPACE = "foo";
    private static final TableReference TABLE_REFERENCE =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "bar");
    private static final Executor TESTING_EXECUTOR = MoreExecutors.directExecutor();


    @Test
    public void testCacheNotBustedGetQuerySpec() {
        QueryCache cache = QueryCache.create(
                ALWAYS_INCREASING_ENTRY_CREATOR,
                TAGGED_METRIC_REGISTRY,
                100);

        GetQuerySpec initialQuerySpec =
                createGetQuerySpec(KEYSPACE, TABLE_REFERENCE, PtBytes.toBytes(10), PtBytes.toBytes(10), 3);

        assertThat(cache.prepare(initialQuerySpec))
                .isEqualTo(cache.prepare(
                        createGetQuerySpec(KEYSPACE, TABLE_REFERENCE, PtBytes.toBytes(10), PtBytes.toBytes(10), 3)));

        assertThat(cache.prepare(initialQuerySpec))
                .isEqualTo(cache.prepare(
                        createGetQuerySpec(KEYSPACE, TABLE_REFERENCE, PtBytes.toBytes(10), PtBytes.toBytes(10), 1)));

        assertThat(cache.prepare(initialQuerySpec))
                .isEqualTo(cache.prepare(
                        createGetQuerySpec(KEYSPACE, TABLE_REFERENCE, PtBytes.toBytes(10), PtBytes.toBytes(7), 3)));

        assertThat(cache.prepare(initialQuerySpec))
                .isEqualTo(cache.prepare(
                        createGetQuerySpec(KEYSPACE, TABLE_REFERENCE, PtBytes.toBytes(4), PtBytes.toBytes(10), 3)));
    }

    private static GetQuerySpec createGetQuerySpec(
            String keyspace,
            TableReference tableReference,
            byte[] rowValue,
            byte[] columnValue,
            int timestamp) {
        return ImmutableGetQuerySpec.builder()
                .keySpace(keyspace)
                .tableReference(tableReference)
                .column(ByteBuffer.wrap(rowValue))
                .row(ByteBuffer.wrap(columnValue))
                .humanReadableTimestamp(timestamp)
                .queryConsistency(READ_CONSISTENCY)
                .executor(TESTING_EXECUTOR)
                .build();
    }
}
