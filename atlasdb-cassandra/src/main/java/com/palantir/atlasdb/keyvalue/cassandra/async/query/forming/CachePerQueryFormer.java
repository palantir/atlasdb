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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Preconditions;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CachePerQueryFormer extends AbstractQueryFormer {

    public static CachePerQueryFormer create(
            TaggedMetricRegistry taggedMetricRegistry,
            Map<SupportedQuery, Integer> cacheSizes) {
        CachePerQueryFormer cachePerQueryForming = create(cacheSizes);

        cachePerQueryForming.requestToCacheMap.forEach((queryType, cache) ->
                registerCache(taggedMetricRegistry, CACHE_NAME_PREFIX + queryType, cache));

        return cachePerQueryForming;
    }

    public static CachePerQueryFormer create(Map<SupportedQuery, Integer> cacheSizes) {
        Preconditions.checkState(cacheSizes.size() == SupportedQuery.values().length,
                "Not all operations have a defined cache size");
        ImmutableMap.Builder<SupportedQuery, Cache<String, String>> builder = ImmutableMap.builder();

        cacheSizes.forEach((queryType, size) -> builder.put(queryType, createCache(size)));

        return new CachePerQueryFormer(builder.build());
    }

    public static CachePerQueryFormer create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        CachePerQueryFormer cachePerQueryForming = create(cacheSize);

        cachePerQueryForming.requestToCacheMap.forEach((queryType, cache) ->
                registerCache(taggedMetricRegistry, CACHE_NAME_PREFIX + queryType, cache));

        return cachePerQueryForming;
    }

    public static CachePerQueryFormer create(int cacheSize) {
        ImmutableMap.Builder<SupportedQuery, Cache<String, String>> builder = ImmutableMap.builder();

        for (SupportedQuery supportedQuery : SupportedQuery.values()) {
            builder.put(supportedQuery, createCache(cacheSize));
        }


        return new CachePerQueryFormer(builder.build());
    }

    private final ImmutableMap<SupportedQuery, Cache<String, String>> requestToCacheMap;

    private CachePerQueryFormer(
            ImmutableMap<SupportedQuery, Cache<String, String>> requestToCacheMap) {
        this.requestToCacheMap = requestToCacheMap;
    }

    @Override
    public final String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference) {
        String normalizedName = AbstractQueryFormer.normalizeName(keySpace, tableReference);
        return requestToCacheMap
                .get(supportedQuery)
                .get(normalizedName, key -> String.format(QUERY_FORMATS_MAP.get(supportedQuery), normalizedName));
    }
}
