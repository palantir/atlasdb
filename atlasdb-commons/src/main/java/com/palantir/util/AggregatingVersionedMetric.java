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

import com.codahale.metrics.Gauge;

/**
 * A Gauge based off of {@link AggregatingVersionedSupplier} that exposes methods to interact with the underlying
 * Supplier.
 */
public class AggregatingVersionedMetric<T> implements Gauge<T> {
    private final AggregatingVersionedSupplier<T> delegate;

    public AggregatingVersionedMetric(AggregatingVersionedSupplier<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T getValue() {
        return delegate.get().value();
    }

    public void update(Integer key, T value) {
        delegate.update(key, value);
    }

    public VersionedType<T> getVersionedValue() {
        return delegate.get();
    }

    public T getLastValueForKey(Integer key) {
        return delegate.getLastValueForKey(key);
    }
}
