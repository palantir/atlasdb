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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class QueryCache<R> {
    public interface EntryCreator<R> {
        R createEntry(CqlQuerySpec querySpec);
    }

    private static final String CACHE_NAME_PREFIX = MetricRegistry.name(
            QueryCache.class,
            "prepared",
            "statements");

    public static <R> QueryCache<R> create(
            EntryCreator<R> entryCreator,
            TaggedMetricRegistry taggedMetricRegistry,
            int cacheSize) {
        Cache<CqlQuerySpec, R> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, CACHE_NAME_PREFIX);
        return new QueryCache<>(entryCreator, cache);
    }

    private final EntryCreator<R> entryCreator;
    private final Cache<CqlQuerySpec, R> cache;

    private QueryCache(EntryCreator<R> entryCreator, Cache<CqlQuerySpec, R> cache) {
        this.entryCreator = entryCreator;
        this.cache = cache;
    }

    R cacheQuerySpec(CqlQuerySpec spec) {
        return cache.get(spec, entryCreator::createEntry);
    }
}
