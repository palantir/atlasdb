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

package com.palantir.atlasdb.coordination;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;

public class CoordinationServiceImpl<T> implements CoordinationService<T> {
    private final CoordinationStore<T> store;
    private final AtomicReference<ValueAndBound<T>> cache = new AtomicReference<>(getInitialCacheValue());

    public CoordinationServiceImpl(CoordinationStore<T> store) {
        this.store = store;
    }

    @Override
    public Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp) {
        ValueAndBound<T> cachedReference = cache.get();
        if (cachedReference.bound() < timestamp) {
            return readLatestValueFromStore()
                    .filter(valueAndBound -> valueAndBound.bound() >= timestamp);
        }
        return Optional.of(cachedReference);
    }

    @Override
    public CheckAndSetResult<ValueAndBound<T>> tryTransformCurrentValue(Function<Optional<T>, T> transform) {
        CheckAndSetResult<ValueAndBound<T>> transformResult = store.transformAgreedValue(transform);
        ValueAndBound<T> existingValue = Iterables.getOnlyElement(transformResult.existingValues());
        accumulateCachedValue(Optional.of(existingValue));
        return transformResult;
    }

    private Optional<ValueAndBound<T>> readLatestValueFromStore() {
        Optional<ValueAndBound<T>> storedValueAndBound = store.getAgreedValue();
        accumulateCachedValue(storedValueAndBound);
        return storedValueAndBound;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void accumulateCachedValue(Optional<ValueAndBound<T>> valueAndBound) {
        valueAndBound.ifPresent(presentValue ->
                cache.accumulateAndGet(presentValue, this::chooseValueWithGreaterBound)
        );
    }

    private ValueAndBound<T> chooseValueWithGreaterBound(
            ValueAndBound<T> valueAndBound1,
            ValueAndBound<T> valueAndBound2) {
        if (valueAndBound1.bound() > valueAndBound2.bound()) {
            return valueAndBound1;
        }
        return valueAndBound2;
    }

    private static <T> ValueAndBound<T> getInitialCacheValue() {
        return ValueAndBound.of(Optional.empty(), ValueAndBound.INVALID_BOUND);
    }
}
