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

package com.palantir.atlasdb.keyvalue.pue;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.function.Function;

public class ComplexPutUnlessExistsTable<T> implements PutUnlessExistsTable<T> {
    private final ConsensusForgettingPutUnlessExistsStore store;
    private final ValueSerializers<T> valueSerializers;
    private final Function<Optional<T>, T> fallbackValueFunction;

    private ComplexPutUnlessExistsTable(
            ConsensusForgettingPutUnlessExistsStore store,
            ValueSerializers<T> valueSerializers,
            Function<Optional<T>, T> fallbackValueFunction) {
        this.store = store;
        this.valueSerializers = valueSerializers;
        this.fallbackValueFunction = fallbackValueFunction;
    }

    public static <T> ComplexPutUnlessExistsTable<T> create(
            KeyValueService keyValueService,
            TableReference tableReference,
            ValueSerializers<T> valueSerializers,
            Function<Optional<T>, T> fallbackValueFunction) {
        return new ComplexPutUnlessExistsTable<>(
                new ConsensusForgettingPutUnlessExistsStore(keyValueService, tableReference),
                valueSerializers,
                fallbackValueFunction);
    }

    @Override
    public T get(Cell c) {
        Optional<PutUnlessExistsState> currentState = store.get(c);
        if (currentState.isPresent()) {
            PutUnlessExistsState state = currentState.get();
            if (state.commitState() == CommitState.COMMITTED) {
                return valueSerializers.byteDeserializer().apply(state.value().asNewByteArray());
            } else if (state.commitState() == CommitState.PENDING) {
                store.checkAndSet(
                        c,
                        state,
                        ImmutablePutUnlessExistsState.builder()
                                .value(Bytes.from(valueSerializers
                                        .byteSerializer()
                                        .apply(fallbackValueFunction.apply(
                                                Optional.of(state.value()).map(v -> valueSerializers
                                                        .byteDeserializer()
                                                        .apply(v.asNewByteArray()))))))
                                .commitState(CommitState.PENDING)
                                .build());
                store.put(c, ImmutablePutUnlessExistsState.builder().build());
            } else {
                throw new SafeIllegalStateException(
                        "Shouldn't be here?", SafeArg.of("commitState", state.commitState()));
            }
        }
        // currentState empty - no value yet

        try {
            T emptyFallbackValue = fallbackValueFunction.apply(Optional.empty());
            putUnlessExists(c, emptyFallbackValue);
            return emptyFallbackValue;
        } catch (KeyAlreadyExistsException e) {
            return get(c);
        }
    }

    @Override
    public void putUnlessExists(Cell c, T value) throws KeyAlreadyExistsException {
        store.putUnlessExists(
                c,
                ImmutablePutUnlessExistsState.builder()
                        .value(Bytes.from(valueSerializers.byteSerializer().apply(value)))
                        .commitState(CommitState.PENDING)
                        .build());

        // TODO (jkong): Straight put of committed
    }
}
