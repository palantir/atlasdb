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

import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.util.function.Consumer;
import java.util.function.Function;

/***
 * This refreshable allows you to specify an initial default value if the initial mapping from another refreshable
 * fails.
 *
 * {@link CassandraAtlasDbFactory} creates a default runtime config if the initial runtime config is not
 * valid (i.e the refreshable map function throws an exception), and uses the previous refreshable value if a
 * subsequent config is invalid.
 *
 * Whilst you could achieve the first requirement with standard refreshables (have the refreshable#map catch the
 * exception and return the default), this does not provide the second requirement.
 *
 * Note: This is only required while {@link CassandraKeyValueServiceRuntimeConfig} has a default. Soon, the runtime
 * config will be required, and so we can simply map (and throw) from the underlying KVS runtime config.
 */
final class RefreshableWithInitialValue<T> implements Refreshable<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(RefreshableWithInitialValue.class);

    private final SettableRefreshable<T> delegate;

    private <K> RefreshableWithInitialValue(Refreshable<K> refreshable, Function<K, T> mapper, T initialValue) {
        delegate = Refreshable.create(initialValue);
        refreshable.subscribe(value -> {
            try {
                T newValue = mapper.apply(value);
                delegate.update(newValue);
            } catch (Exception e) {
                log.warn("Failed to update refreshable", e);
            }
        });
    }

    public static <K, T> RefreshableWithInitialValue<T> of(
            Refreshable<K> refreshable, Function<K, T> mapper, T initialValue) {
        return new RefreshableWithInitialValue<>(refreshable, mapper, initialValue);
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
