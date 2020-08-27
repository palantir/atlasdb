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
package com.palantir.util;

import com.google.common.base.Suppliers;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class AggregatingVersionedSupplier<T> implements Supplier<VersionedType<T>> {
    public static final long UNINITIALIZED_VERSION = 0L;

    private final Function<Collection<T>, T> aggregator;
    private final Supplier<VersionedType<T>> memoizedValue;
    private volatile long version = UNINITIALIZED_VERSION;

    private final ConcurrentMap<Integer, T> latestValues = new ConcurrentHashMap<>();

    /**
     * Creates a supplier that returns a {@link VersionedType} containing the result of applying the given aggregating
     * function to a collection of values maintained in an internal map. The return value is memoized for the specified
     * amount of time after which a call to get() will recompute the result and increase the version of the result.
     *
     * @param aggregator the aggregating function to use.
     * @param expirationMillis amount of time in milliseconds after which a call to get() will recompute the result of
     * applying aggregator and increase the returned version.
     */
    public AggregatingVersionedSupplier(Function<Collection<T>, T> aggregator, long expirationMillis) {
        this.aggregator = aggregator;
        this.memoizedValue = Suppliers
                .memoizeWithExpiration(this::recalculate, expirationMillis, TimeUnit.MILLISECONDS);
    }

    public static <C extends Comparable<C>> AggregatingVersionedSupplier<C> min(long expirationMillis) {
        return new AggregatingVersionedSupplier<>(AggregatingVersionedSupplier::min, expirationMillis);
    }

    private static <C extends Comparable<C>> C min(Collection<C> currentValues) {
        return currentValues.stream().min(Comparator.naturalOrder()).orElse(null);
    }

    /**
     * Insert, or replace, a (key, value) pair into the internal map.
     */
    public void update(Integer key, T value) {
        latestValues.put(key, value);
    }

    private VersionedType<T> recalculate() {
        version++;
        return VersionedType.of(aggregator.apply(latestValues.values()), version);
    }

    @Override
    public VersionedType<T> get() {
        return memoizedValue.get();
    }
}
