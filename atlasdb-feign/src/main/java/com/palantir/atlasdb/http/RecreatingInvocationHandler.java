/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.AbstractInvocationHandler;
import com.google.common.reflect.Reflection;

public class RecreatingInvocationHandler<T, D> extends AbstractInvocationHandler {

    private final Supplier<Optional<T>> deltaSupplier;
    private final Function<T, D> delegateCreator;

    private volatile D activeDelegate;

    private RecreatingInvocationHandler(Supplier<Optional<T>> supplier, Function<T, D> delegateCreator) {
        this.deltaSupplier = supplier;
        this.delegateCreator = delegateCreator;

        T initialValue = supplier.get().orElseThrow(() ->
                new IllegalStateException("First return value of supplier should be nonempty"));
        activeDelegate = delegateCreator.apply(initialValue);
    }

    public static <T, D> D create(
            Supplier<T> supplier,
            Function<T, D> delegateCreator,
            Class<D> delegateType) {
        return createWithRawDeltaSupplier(wrapInDeltaSupplier(supplier), delegateCreator, delegateType);
    }

    @VisibleForTesting
    static <T, D> D createWithRawDeltaSupplier(
            Supplier<Optional<T>> supplier,
            Function<T, D> delegateCreator,
            Class<D> delegateType) {
        return Reflection.newProxy(delegateType, new RecreatingInvocationHandler<>(supplier, delegateCreator));
    }

    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        updateDelegateIfNeeded();
        try {
            return method.invoke(activeDelegate, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void updateDelegateIfNeeded() {
        deltaSupplier.get().ifPresent(t -> activeDelegate = delegateCreator.apply(t));
    }

    @VisibleForTesting
    static <T> Supplier<Optional<T>> wrapInDeltaSupplier(Supplier<T> supplier) {
        return new DeltaSupplier<>(supplier);
    }

    private static class DeltaSupplier<T> implements Supplier<Optional<T>> {
        private final Supplier<T> baseSupplier;
        private final AtomicReference<T> lastSeenValue = new AtomicReference<>();

        private DeltaSupplier(Supplier<T> baseSupplier) {
            this.baseSupplier = baseSupplier;
        }

        @Override
        public Optional<T> get() {
            T actualValue = baseSupplier.get();
            T currentLastSeenValue = lastSeenValue.get();
            while (!actualValue.equals(currentLastSeenValue)) {
                if (lastSeenValue.compareAndSet(currentLastSeenValue, actualValue)) {
                    return Optional.of(actualValue);
                }
                currentLastSeenValue = lastSeenValue.get();
            }
            // We didn't change the value.
            return Optional.empty();
        }
    }
}
