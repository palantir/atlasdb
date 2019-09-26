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

import java.util.stream.Stream;

import org.junit.Test;

import com.datastax.driver.core.Row;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.async.QueryCache.EntryCreator;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public class QueryCacheTest {

    private static Integer counter = 0;
    private static final EntryCreator<Integer> ALWAYS_INCREASING_ENTRY_CREATOR = querySpec -> counter = counter + 1;

    private static final MetricsManager METRICS_MANAGER = MetricsManagers.createForTests();
    private static final TaggedMetricRegistry TAGGED_METRIC_REGISTRY = METRICS_MANAGER.getTaggedRegistry();

    private static final String NAMESPACE = "foo";
    private static final TableReference TABLE_REFERENCE =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "bar");

    private static final RowStreamAccumulator<Object> firstAccumulator = new RowStreamAccumulator<Object>() {
        @Override
        public void accumulateRowStream(Stream<Row> rowStream) {
        }

        @Override
        public Object result() {
            return null;
        }
    };
    private static final RowStreamAccumulator<Object> secondAccumulator = new RowStreamAccumulator<Object>() {
        @Override
        public void accumulateRowStream(Stream<Row> rowStream) {
        }

        @Override
        public Object result() {
            return null;
        }
    };

    @Test
    public void testCacheNotBusted() {
        QueryCache<Integer> cache = QueryCache.create(
                ALWAYS_INCREASING_ENTRY_CREATOR,
                TAGGED_METRIC_REGISTRY,
                100);
        CqlQuerySpec firstSpec = ImmutableCqlQuerySpec.builder()
                .keySpace(NAMESPACE)
                .supportedQuery(SupportedQuery.GET)
                .tableReference(TABLE_REFERENCE)
                .rowStreamAccumulatorFactory(() -> firstAccumulator).build();
        CqlQuerySpec secondSpec = ImmutableCqlQuerySpec.builder()
                .keySpace(NAMESPACE)
                .supportedQuery(SupportedQuery.GET)
                .tableReference(TABLE_REFERENCE)
                .rowStreamAccumulatorFactory(() -> secondAccumulator).build();

        assertThat(cache.cacheQuerySpec(firstSpec)).isEqualTo(cache.cacheQuerySpec(secondSpec));
    }
}
