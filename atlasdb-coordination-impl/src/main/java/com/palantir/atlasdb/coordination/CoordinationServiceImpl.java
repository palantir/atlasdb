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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.immutables.value.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.remoting3.ext.jackson.ObjectMappers;

import okio.ByteString;

public class CoordinationServiceImpl<T> implements CoordinationService<T> {
    private static final ObjectMapper OBJECT_MAPPER = ObjectMappers.newServerObjectMapper();

    private final CoordinationStore store;
    private final AtomicReference<SequenceAndValueAndBound<T>> cache = new AtomicReference<>(getInitialCacheValue());

    private final Class<T> clazz;
    private final LongSupplier sequenceNumberSupplier;

    public CoordinationServiceImpl(CoordinationStore store, Class<T> clazz, LongSupplier sequenceNumberSupplier) {
        this.store = store;
        this.clazz = clazz;
        this.sequenceNumberSupplier = sequenceNumberSupplier;
    }

    @Override
    public Optional<ValueAndBound<T>> getValueForTimestamp(long timestamp) {
        SequenceAndValueAndBound<T> cachedReference = cache.get();
        if (cachedReference == null || cachedReference.bound() < timestamp) {
            return store.getCoordinationValue()
                    .filter(sequenceAndBound -> sequenceAndBound.bound() >= timestamp)
                    .map(this::getCorrespondingValueFromStore);
        }
        // TODO (jkong): Intern if performance is bad
        return Optional.of(ImmutableValueAndBound.of(cachedReference.value(), cachedReference.bound()));
    }

    @Override
    public boolean tryTransformCurrentValue(
            Function<Optional<ValueAndBound<T>>, ValueAndBound<T>> transform) {
        Optional<SequenceAndBound> sequenceAndBound = store.getCoordinationValue();
        Optional<ValueAndBound<T>> valueAndBound = sequenceAndBound.map(this::getCorrespondingValueFromStore);

        ValueAndBound<T> target = transform.apply(valueAndBound);
        if (shouldUpdateStore(valueAndBound, target)) {
            long sequenceNumber = sequenceNumberSupplier.getAsLong();
            store.putValue(sequenceNumber, serializeByteString(
                    target.value().orElseThrow(() -> new IllegalStateException("Cannot put an empty value"))));
            return store.checkAndSetCoordinationValue(sequenceAndBound,
                    ImmutableSequenceAndBound.of(sequenceNumber, target.bound()))
                    .successful();
        }
        return false;
    }

    private boolean shouldUpdateStore(Optional<ValueAndBound<T>> existing, ValueAndBound<T> target) {
        return existing.map(valueAndBound -> target.bound() > valueAndBound.bound()).orElse(true);
    }

    private static <T> SequenceAndValueAndBound<T> getInitialCacheValue() {
        return ImmutableSequenceAndValueAndBound.of(
                SequenceAndValueAndBound.INVALID_SEQUENCE, Optional.empty(), SequenceAndValueAndBound.INVALID_BOUND);
    }

    private ValueAndBound<T> getCorrespondingValueFromStore(SequenceAndBound sequenceAndBound) {
        return ImmutableValueAndBound.of(readValueFromStore(sequenceAndBound), sequenceAndBound.bound());
    }

    private Optional<T> readValueFromStore(SequenceAndBound presentSequenceAndBound) {
        Optional<T> valueOptional = store.getValue(presentSequenceAndBound.sequence()).map(this::deserializeByteString);
        accumulateCachedValue(presentSequenceAndBound, valueOptional);
        return valueOptional;
    }

    private void accumulateCachedValue(SequenceAndBound presentSequenceAndBound, Optional<T> valueOptional) {
        ImmutableSequenceAndValueAndBound<T> newCacheValue =
                ImmutableSequenceAndValueAndBound.of(
                        presentSequenceAndBound.sequence(),
                        valueOptional,
                        presentSequenceAndBound.bound());
        cache.accumulateAndGet(newCacheValue, (svb1, svb2) -> {
            if (svb1.bound() > svb2.bound()) {
                return svb1;
            }
            return svb2;
        });
    }

    private ByteString serializeByteString(T object) {
        try {
            return ByteString.of(OBJECT_MAPPER.writeValueAsBytes(object));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private T deserializeByteString(ByteString byteString) {
        try {
            return OBJECT_MAPPER.readValue(byteString.toByteArray(), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Value.Immutable
    interface SequenceAndValueAndBound<T> {
        long INVALID_SEQUENCE = -1;
        long INVALID_BOUND = -1;

        @Value.Parameter
        long sequence();
        @Value.Parameter
        Optional<T> value();
        @Value.Parameter
        long bound();
    }
}
