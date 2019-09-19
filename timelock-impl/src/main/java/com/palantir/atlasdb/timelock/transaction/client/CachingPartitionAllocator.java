/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.client;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class CachingPartitionAllocator<T> implements NumericPartitionAllocator<T> {
    private final LoadingCache<T, Integer> loadingCache;

    @VisibleForTesting
    CachingPartitionAllocator(
            DistributingModulusGenerator modulusGenerator,
            Executor executor,
            Ticker ticker,
            Duration timeoutAfterAccess) {
        this.loadingCache = Caffeine.newBuilder()
                .expireAfterAccess(timeoutAfterAccess)
                .executor(executor)
                .ticker(ticker)
                .removalListener(
                        (T key, Integer value, RemovalCause cause) -> modulusGenerator.unmarkResidue(value))
                .build(unused -> modulusGenerator.getAndMarkResidue());
    }

    public static <T> CachingPartitionAllocator<T> createDefault(int numModuli) {
        // TODO (jkong): This should ideally be configurable.
        DistributingModulusGenerator modulusGenerator = new DistributingModulusGenerator(numModuli);
        return new CachingPartitionAllocator<>(
                modulusGenerator,
                ForkJoinPool.commonPool(),
                Ticker.systemTicker(),
                Duration.of(5, ChronoUnit.MINUTES));
    }

    @Override
    public List<Integer> getRelevantModuli(T object) {
        return ImmutableList.of(loadingCache.get(object));
    }
}
