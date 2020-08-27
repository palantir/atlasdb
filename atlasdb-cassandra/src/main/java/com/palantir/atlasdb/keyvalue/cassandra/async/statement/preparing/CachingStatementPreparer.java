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

package com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.QueryType;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import org.immutables.value.Value;

public final class CachingStatementPreparer implements StatementPreparer {

    private static final String CACHE_NAME_PREFIX = MetricRegistry.name(
            CachingStatementPreparer.class,
            "prepared",
            "statements");

    public static CachingStatementPreparer create(
            StatementPreparer statementPreparer,
            TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        Cache<CacheKey, PreparedStatement> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, CACHE_NAME_PREFIX);
        return new CachingStatementPreparer(statementPreparer, cache);
    }

    private final StatementPreparer statementPreparer;
    private final Cache<CacheKey, PreparedStatement> cache;

    private CachingStatementPreparer(
            StatementPreparer statementPreparer,
            Cache<CacheKey, PreparedStatement> cache) {
        this.statementPreparer = statementPreparer;
        this.cache = cache;
    }

    @Override
    public PreparedStatement prepare(CqlQuerySpec querySpec) {
        CacheKey cacheKey = ImmutableCacheKey.builder()
                .cqlQueryContext(querySpec.cqlQueryContext())
                .queryType(querySpec.queryType())
                .build();
        return cache.get(cacheKey, key -> statementPreparer.prepare(querySpec));
    }

    @Value.Immutable
    interface CacheKey {
        CqlQueryContext cqlQueryContext();

        QueryType queryType();
    }
}
