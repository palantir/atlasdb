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

/**
 * Utility class to make manipulating {@link io.vavr.collection.Map} and {@link io.vavr.collection.Set} less
 * error-prone (puts return a *new* instance).
 */
final class StructureHolder<V> {
    private final V initialValue;
    private V structure;

    private StructureHolder(V initialValue) {
        this.initialValue = initialValue;
        this.structure = initialValue;
    }

    synchronized void with(Function<V, V> mutator) {
        structure = mutator.apply(structure);
    }

    synchronized V getStructure() {
        return structure;
    }

    synchronized <K> K apply(Function<V, K> function) {
        return function.apply(structure);
    }

    synchronized void resetToInitialValue() {
        structure = initialValue;
    }

    static <V> StructureHolder<V> create(V initialValue) {
        return new StructureHolder<>(initialValue);
    }
}
