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
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class UnifiedCacheQueryFormer implements QueryFormer {

    @Value.Immutable
    interface CacheKey {
        @Value.Parameter
        String keySpace();

        @Value.Parameter
        TableReference tableReference();

        @Value.Parameter
        SupportedQuery supportedQuery();

        @Value.Derived
        default String fullyQualifiedName() {
            return keySpace() + "." + tableReference().getQualifiedName();
        }

        @Value.Derived
        default String formattedQuery() {
            return supportedQuery().formQueryString(fullyQualifiedName());
        }
    }

    public static QueryFormer create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        return new UnifiedCacheQueryFormer(
                QueryFormer.registerCache(
                        taggedMetricRegistry,
                        CACHE_NAME_PREFIX + "ALL",
                        QueryFormer.createCache(cacheSize)));
    }

    private final Cache<CacheKey, String> cache;

    private UnifiedCacheQueryFormer(Cache<CacheKey, String> cache) {
        this.cache = cache;
    }

    @Override
    public String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference) {
        CacheKey cacheKey = ImmutableCacheKey.builder()
                .supportedQuery(supportedQuery)
                .keySpace(keySpace)
                .tableReference(tableReference)
                .build();
        return cache.get(cacheKey, CacheKey::formattedQuery);
    }
}
