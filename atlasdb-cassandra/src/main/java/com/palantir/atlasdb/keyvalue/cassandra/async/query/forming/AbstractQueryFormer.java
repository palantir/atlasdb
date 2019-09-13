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

abstract class AbstractQueryFormer implements QueryFormer {

    static final String CACHE_NAME_PREFIX = "query.async.prepared.statements.cache.metrics.";

    static void registerCache(TaggedMetricRegistry taggedMetricRegistry, String name, Cache<String, String> cache) {
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, name);
    }

    static Cache<String, String> createCache(int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    // TODO (OStevan): prone to injection, fix this with some pattern match checking
    static String normalizeName(String keyspace, TableReference tableReference) {
        return keyspace + "." + tableReference.getQualifiedName();
    }
}
