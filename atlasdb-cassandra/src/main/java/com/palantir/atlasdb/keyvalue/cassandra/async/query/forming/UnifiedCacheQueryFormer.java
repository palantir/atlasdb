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

import com.github.benmanes.caffeine.cache.Cache;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;


public final class UnifiedCacheQueryFormer extends AbstractQueryFormer {


    public static UnifiedCacheQueryFormer create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        UnifiedCacheQueryFormer unifiedCacheQueryForming = create(cacheSize);
        AbstractQueryFormer.registerCache(taggedMetricRegistry, CACHE_NAME_PREFIX + "ALL",
                unifiedCacheQueryForming.cache);
        return unifiedCacheQueryForming;
    }

    public static UnifiedCacheQueryFormer create(int cacheSize) {
        return new UnifiedCacheQueryFormer(AbstractQueryFormer.createCache(cacheSize));
    }

    private final Cache<String, String> cache;

    private UnifiedCacheQueryFormer(Cache<String, String> cache) {
        this.cache = cache;
    }

    @Override
    public String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference) {
        String normalizedName = AbstractQueryFormer.normalizeName(keySpace, tableReference);
        String cacheKey = supportedQuery.toString() + '.' + normalizedName;
        return cache.get(cacheKey, key -> String.format(QUERY_FORMATS_MAP.get(supportedQuery), normalizedName));
    }
}
