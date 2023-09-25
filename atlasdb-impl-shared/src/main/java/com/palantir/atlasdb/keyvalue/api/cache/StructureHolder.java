/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class to make manipulating {@link io.vavr.collection.Map} and {@link io.vavr.collection.Set} less
 * error-prone as all methods that modify state return a *new* instance in vavr.
 */
@ThreadSafe
public final class StructureHolder<V> {
    private final Supplier<V> initialValueSupplier;

    @GuardedBy("this")
    private V structure;

    private StructureHolder(Supplier<V> initialValueSupplier) {
        this.initialValueSupplier = initialValueSupplier;
        this.structure = initialValueSupplier.get();
    }

    public synchronized void with(Function<V, V> mutator) {
        structure = mutator.apply(structure);
    }

    public synchronized V getSnapshot() {
        return structure;
    }

    public synchronized <K> K apply(Function<V, K> function) {
        return function.apply(structure);
    }

    public synchronized void resetToInitialValue() {
        structure = initialValueSupplier.get();
    }

    public static <V> StructureHolder<V> create(Supplier<V> initialValue) {
        return new StructureHolder<>(initialValue);
    }
}
