/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cassandra;

import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public final class RefreshableWithInitialDefault<T> implements Refreshable<T> {
    private final AtomicReference<Optional<T>> latestValidValue = new AtomicReference<>(Optional.empty());
    private final Refreshable<T> delegate;

    private <K> RefreshableWithInitialDefault(Refreshable<K> refreshable, Function<K, T> function, T defaultValue) {
        delegate = refreshable.map(value -> {
            try {
                T newValue = function.apply(value);
                latestValidValue.set(Optional.of(newValue));
                return newValue;
            } catch (Exception e) {
                return latestValidValue.get().orElse(defaultValue);
            }
        });
    }

    public static <K, T> RefreshableWithInitialDefault<T> of(
            Refreshable<K> refreshable, Function<K, T> function, T defaultValue) {
        return new RefreshableWithInitialDefault<>(refreshable, function, defaultValue);
    }

    @Override
    public T current() {
        return delegate.current();
    }

    @Override
    public T get() {
        return delegate.get();
    }

    @Override
    public Disposable subscribe(Consumer<? super T> consumer) {
        return delegate.subscribe(consumer);
    }

    @Override
    public <R> Refreshable<R> map(Function<? super T, R> function) {
        return delegate.map(function);
    }
}
