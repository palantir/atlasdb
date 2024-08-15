/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.base.Suppliers;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Contains the top N elements based on a score, updated eagerly within a memoization interval.
 * During the evaluation of an update, if the top list is perceived to be not full (due to memoization),
 * the current update element is considered to be in the top list.
 */
public final class TopListFilter<K> {
    private final int maxSize;
    private final Supplier<Set<K>> eligibleKeysSupplier;

    private final ConcurrentMap<K, Double> entries = new ConcurrentHashMap<>();
    private final Comparator<Map.Entry<K, Double>> reversedScoreEntryComparator =
            Map.Entry.comparingByValue(Comparator.reverseOrder());

    TopListFilter(int maxSize, Duration resetInterval) {
        this.maxSize = maxSize;
        this.eligibleKeysSupplier = Suppliers.memoizeWithExpiration(this::computeEligibleKeysInternal, resetInterval);
    }

    /**
     * Updates the top list and returns whether the current element is considered to be in the top list.
     */
    boolean updateAndGetStatus(K key, double score) {
        entries.merge(key, score, Math::max);
        Set<K> topKeys = eligibleKeysSupplier.get();

        // returns true if either the key is in the internal
        // top list, or if it is not and the internal top
        // list is not full (due to memoization)
        return topKeys.contains(key) || topKeys.size() < maxSize;
    }

    private Set<K> computeEligibleKeysInternal() {
        Set<K> interesting = entries.entrySet().stream()
                .sorted(reversedScoreEntryComparator)
                .limit(maxSize)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
        entries.clear();
        return interesting;
    }
}
