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

package com.palantir.atlasdb.keyvalue.cassandra.async.query.forming;

import org.immutables.value.Value;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CacheQueryFormer implements QueryFormer {

    private static final String CACHE_NAME = "query.async.query.forming.cache.metrics.all";

    @Value.Immutable
    interface CacheKey {
        @Value.Parameter
        String keySpace();

        @Value.Parameter
        TableReference tableReference();

        @Value.Parameter
        SupportedQuery supportedQuery();
    }

    public static QueryFormer create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        Cache<CacheKey, String> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, CACHE_NAME);
        return new CacheQueryFormer(cache);
    }

    private final Cache<CacheKey, String> cache;

    private CacheQueryFormer(Cache<CacheKey, String> cache) {
        this.cache = cache;
    }

    @Override
    public String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference) {
        CacheKey cacheKey = ImmutableCacheKey.builder()
                .supportedQuery(supportedQuery)
                .keySpace(keySpace)
                .tableReference(tableReference)
                .build();
        return cache.get(cacheKey, key ->
                key.supportedQuery().formQueryString(key.keySpace(),
                        AbstractKeyValueService.internalTableName(key.tableReference())));
    }
}
