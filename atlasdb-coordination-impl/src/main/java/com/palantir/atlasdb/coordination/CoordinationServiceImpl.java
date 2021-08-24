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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class CoordinationServiceImpl<T> implements CoordinationService<T> {
    private static final SafeLogger log = SafeLoggerFactory.get(CoordinationServiceImpl.class);

    private final CoordinationStore<T> store;
    private final AtomicReference<ValueAndBound<T>> cache = new AtomicReference<>(getInitialCacheValue());

    public CoordinationServiceImpl(CoordinationStore<T> store) {
        this.store = store;
    }

    @Override
    public Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp) {
        ValueAndBound<T> cachedReference = cache.get();
        if (cachedReference.bound() < timestamp) {
            return readLatestValueFromStore().filter(valueAndBound -> valueAndBound.bound() >= timestamp);
        }
        return Optional.of(cachedReference);
    }

    /**
     * In case of failure, the returned value and bound are guaranteed to be the ones that were in the KVS at the time
     * of CAS failure only if the implementation of the CoordinationStore makes such guarantees.
     */
    @Override
    public CheckAndSetResult<ValueAndBound<T>> tryTransformCurrentValue(Function<ValueAndBound<T>, T> transform) {
        CheckAndSetResult<ValueAndBound<T>> transformResult = store.transformAgreedValue(transform);
        ValueAndBound<T> existingValue = Iterables.getOnlyElement(transformResult.existingValues());
        accumulateCachedValue(Optional.of(existingValue));
        return transformResult;
    }

    @Override
    public Optional<ValueAndBound<T>> getLastKnownLocalValue() {
        ValueAndBound<T> cachedValue = cache.get();
        return Objects.equals(getInitialCacheValue(), cachedValue) ? Optional.empty() : Optional.of(cachedValue);
    }

    private Optional<ValueAndBound<T>> readLatestValueFromStore() {
        Optional<ValueAndBound<T>> storedValueAndBound = store.getAgreedValue();
        accumulateCachedValue(storedValueAndBound);
        return storedValueAndBound;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private void accumulateCachedValue(Optional<ValueAndBound<T>> valueAndBound) {
        valueAndBound.ifPresent(
                presentValue -> cache.accumulateAndGet(presentValue, this::chooseValueWithGreaterBound));
    }

    private ValueAndBound<T> chooseValueWithGreaterBound(ValueAndBound<T> currentValue, ValueAndBound<T> nextValue) {
        if (currentValue.bound() > nextValue.bound()) {
            return currentValue;
        }
        log.debug(
                "Updating cached coordination value to a new value, valid till {}",
                SafeArg.of("newBound", nextValue.bound()));
        return nextValue;
    }

    private static <T> ValueAndBound<T> getInitialCacheValue() {
        return ValueAndBound.of(Optional.empty(), ValueAndBound.INVALID_BOUND);
    }
}
