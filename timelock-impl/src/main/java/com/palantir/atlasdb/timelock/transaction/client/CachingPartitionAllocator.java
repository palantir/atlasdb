/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.timelock.transaction.client;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class CachingPartitionAllocator<T> implements NumericPartitionAllocator<T> {
    private final LoadingCache<T, Integer> loadingCache;

    @VisibleForTesting
    CachingPartitionAllocator(LoadingCache<T, Integer> loadingCache) {
        this.loadingCache = loadingCache;
    }

    public static <T> NumericPartitionAllocator<T> createDefault(int numModuli) {
        // TODO (jkong): This should ideally be configurable.
        DistributingModulusGenerator modulusGenerator = DistributingModulusGenerator.create(numModuli);
        return new CachingPartitionAllocator<>(
                Caffeine.newBuilder()
                        .expireAfterAccess(5, TimeUnit.MINUTES)
                        .removalListener(
                                (T key, Integer value, RemovalCause cause) -> modulusGenerator.unmarkResidue(value))
                        .build(unused -> modulusGenerator.getAndMarkResidue()));
    }

    @Override
    public List<Integer> getRelevantModuli(T object) {
        return ImmutableList.of(loadingCache.get(object));
    }
}
