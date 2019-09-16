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

public interface QueryFormer {

    enum SupportedQuery {
        TIME("TIME", "SELECT dateof(now()) FROM system.local ;"),
        GET("GET", "SELECT value, column2 FROM %s "
                + "WHERE key = :key AND column1 = :column1 AND column2 > :column2 ;");

        public final String name;
        public final String format;

        SupportedQuery(String name, String format) {
            this.name = name;
            this.format = format;
        }

        String formQueryString(String fullyQualifiedName) {
            return String.format(this.format, fullyQualifiedName);
        }
    }

    String CACHE_NAME_PREFIX = "query.async.query.forming.cache.metrics.";

    static <K, V> Cache<K, V> registerCache(TaggedMetricRegistry taggedMetricRegistry, String name, Cache<K, V> cache) {
        CaffeineCacheStats.registerCache(taggedMetricRegistry, cache, name);
        return cache;
    }

    static <K, V> Cache<K, V> createCache(int cacheSize) {
        return Caffeine.newBuilder().maximumSize(cacheSize).build();
    }

    String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference);
}
