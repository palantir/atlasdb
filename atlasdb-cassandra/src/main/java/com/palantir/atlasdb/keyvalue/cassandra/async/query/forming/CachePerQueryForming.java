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

import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.Preconditions;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CachePerQueryForming extends AbstractQueryForming {

    public static CachePerQueryForming create(
            TaggedMetricRegistry taggedMetricRegistry,
            Map<SupportedQuery, Integer> cacheSizes) {
        CachePerQueryForming cachePerQueryForming = create(cacheSizes);

        cachePerQueryForming.requestToCacheMap.forEach((queryType, cache) ->
                registerCache(taggedMetricRegistry, CACHE_NAME_PREFIX + queryType, cache));

        return cachePerQueryForming;
    }

    public static CachePerQueryForming create(Map<SupportedQuery, Integer> cacheSizes) {
        Preconditions.checkState(cacheSizes.size() == SupportedQuery.values().length,
                "Not all operations have a defined cache size");
        ImmutableMap.Builder<SupportedQuery, Cache<String, String>> builder = ImmutableMap.builder();

        cacheSizes.forEach((queryType, size) -> builder.put(queryType, createCache(size)));

        return new CachePerQueryForming(builder.build());
    }

    public static CachePerQueryForming create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        CachePerQueryForming cachePerQueryForming = create(cacheSize);

        cachePerQueryForming.requestToCacheMap.forEach((queryType, cache) ->
                registerCache(taggedMetricRegistry, CACHE_NAME_PREFIX + queryType, cache));

        return cachePerQueryForming;
    }

    public static CachePerQueryForming create(int cacheSize) {
        ImmutableMap.Builder<SupportedQuery, Cache<String, String>> builder = ImmutableMap.builder();

        for (SupportedQuery supportedQuery : SupportedQuery.values()) {
            builder.put(supportedQuery, createCache(cacheSize));
        }


        return new CachePerQueryForming(builder.build());
    }

    private final ImmutableMap<SupportedQuery, Cache<String, String>> requestToCacheMap;

    private CachePerQueryForming(
            ImmutableMap<SupportedQuery, Cache<String, String>> requestToCacheMap) {
        this.requestToCacheMap = requestToCacheMap;
    }

    @Override
    String registerFormed(SupportedQuery supportedQuery, String normalizedName, String queryString) {
        return requestToCacheMap
                .get(supportedQuery)
                .get(normalizedName, key -> queryString);
    }
}
