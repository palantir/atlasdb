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

package com.palantir.util;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

public class AggregatingVersionedSupplier implements Supplier<VersionedLong> {
    private final Function<Collection<Long>, Long> aggregator;
    private final Supplier<VersionedLong> memoizedValue;
    private volatile long version = 0;

    private final ConcurrentMap<Integer, Long> latestValues = new ConcurrentHashMap<>();

    public AggregatingVersionedSupplier(Function<Collection<Long>, Long> aggregator, long expirationMillis) {
        this.aggregator = aggregator;
        this.memoizedValue = Suppliers
                .memoizeWithExpiration(this::recalculate, expirationMillis, TimeUnit.MILLISECONDS);
    }

    public void update(Integer key, Long value) {
        latestValues.put(key, value);
    }

    private VersionedLong recalculate() {
        version++;
        return VersionedLong.of(aggregator.apply(latestValues.values()), version);
    }

    @Override
    public VersionedLong get() {
        return memoizedValue.get();
    }
}
