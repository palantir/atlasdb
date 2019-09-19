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

package com.palantir.atlasdb.util;

import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.palantir.tritium.metrics.registry.MetricName;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Cache to avoid excessive string concatenation in MetricRegistry.name(clazz, metricName). */
final class MetricNameCache {

    private final LoadingCache<Key, MetricName> cache;

    MetricNameCache() {
        this.cache = Caffeine.newBuilder()
                .build(new CacheLoader<Key, MetricName>() {
                    @Nullable
                    @Override
                    public MetricName load(@Nonnull Key key) {
                        return MetricName.builder()
                                .safeName(MetricRegistry.name(key.getClazz(), key.getMetricName()))
                                .safeTags(key.getTags())
                                .build();
                    }
                });
    }

    MetricName get(Class<?> clazz, String metricName, Map<String, String> tags) {
        return cache.get(ImmutableKey.of(clazz, metricName, tags));
    }

    void invalidate() {
        cache.invalidateAll();
    }

    @Value.Immutable
    interface Key {

        @Value.Parameter
        Class<?> getClazz();

        @Value.Parameter
        String getMetricName();

        @Value.Parameter
        Map<String, String> getTags();
    }
}
