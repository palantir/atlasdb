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

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

public final class CachePerOperationQueryForming extends AbstractQueryForming {

    public static CachePerOperationQueryForming create(TaggedMetricRegistry taggedMetricRegistry, int cacheSize) {
        return new CachePerOperationQueryForming(
                Stream.of(SupportedQueries.values()).collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                Function.identity(),
                                operation -> createAndRegisterCache(taggedMetricRegistry, operation, cacheSize)
                        ),
                        ImmutableMap::copyOf
                        )
                ));
    }

    public static CachePerOperationQueryForming create(int cacheSize) {
        return new CachePerOperationQueryForming(
                Stream.of(SupportedQueries.values()).collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                Function.identity(),
                                operation -> createCache(cacheSize)
                        ),
                        ImmutableMap::copyOf
                        )
                ));
    }

    private final ImmutableMap<SupportedQueries, Cache<String, String>> requestToCacheMap;

    private CachePerOperationQueryForming(
            ImmutableMap<SupportedQueries, Cache<String, String>> requestToCacheMap) {
        this.requestToCacheMap = requestToCacheMap;
    }

    @Override
    String registerFormed(SupportedQueries supportedQuery, String normalizedName, String queryString) {
        return requestToCacheMap.get(supportedQuery).get(normalizedName,
                key -> queryString);
    }
}
