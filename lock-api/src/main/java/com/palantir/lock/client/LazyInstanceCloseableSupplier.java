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

package com.palantir.lock.client;

import com.google.common.base.Suppliers;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.function.Supplier;

public final class LazyInstanceCloseableSupplier<T extends AutoCloseable> implements CloseableSupplier<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(LazyInstanceCloseableSupplier.class);

    private final Supplier<T> delegate;

    private LazyInstanceCloseableSupplier(Supplier<T> delegate) {
        this.delegate = delegate;
    }

    public static <T extends AutoCloseable> CloseableSupplier<T> of(Supplier<T> delegate) {
        return new LazyInstanceCloseableSupplier<>(Suppliers.memoize(delegate::get));
    }

    @Override
    public T getDelegate() {
        return delegate.get();
    }

    @Override
    public void close() {
        try {
            getDelegate().close();
        } catch (Exception e) {
            log.warn("There was a problem closing delegate - ", e);
        }
    }
}
