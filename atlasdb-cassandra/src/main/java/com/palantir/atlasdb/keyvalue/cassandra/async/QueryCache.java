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

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.atlasdb.keyvalue.cassandra.async.query.CqlQuerySpec;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class QueryCache implements StatementPreparer {

    private static final String CACHE_NAME_PREFIX = MetricRegistry.name(
            QueryCache.class,
            "prepared",
            "statements");

    public static  QueryCache create(
            StatementPreparer statementPreparer,
            TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        Cache<CqlQuerySpec, PreparedStatement> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, CACHE_NAME_PREFIX);
        return new QueryCache(statementPreparer, cache);
    }

    private final StatementPreparer statementPreparer;
    private final Cache<CqlQuerySpec, PreparedStatement> cache;

    private QueryCache(StatementPreparer statementPreparer, Cache<CqlQuerySpec, PreparedStatement> cache) {
        this.statementPreparer = statementPreparer;
        this.cache = cache;
    }

    @Override
    public PreparedStatement prepare(CqlQuerySpec querySpec) {
        return cache.get(querySpec, statementPreparer::prepare);
    }
}
