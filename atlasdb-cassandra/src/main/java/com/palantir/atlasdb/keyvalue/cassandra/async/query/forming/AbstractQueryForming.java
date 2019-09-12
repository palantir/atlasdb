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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.caffeine.CaffeineCacheStats;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public abstract class AbstractQueryForming implements QueryFormer {
    static Cache<String, String> createAndRegisterCache(TaggedMetricRegistry taggedMetricRegistry,
            SupportedQueries operation, int cacheSize) {
        Cache<String, String> cache = Caffeine.newBuilder().maximumSize(cacheSize).build();
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache,
                "query.async.prepared.statements.cache.metrics." + operation);
        return cache;
    }

    static Cache<String, String> createCache(int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    // TODO (OStevan): prone to injection, fix this with some pattern match checking
    private static String normalizeName(String keyspace, TableReference tableReference) {
        return keyspace + "." + tableReference.getQualifiedName();
    }

    abstract String registerFormed(SupportedQueries supportedQuery, String normalizedName, String queryString);

    @Override
    public String formQuery(SupportedQueries supportedQuery, String keySpace, TableReference tableReference) {
        String normalizedName = AbstractQueryForming.normalizeName(keySpace, tableReference);
        return registerFormed(supportedQuery, normalizedName,
                String.format(QUERY_FORMATS_MAP.get(supportedQuery), normalizedName));
    }
}
