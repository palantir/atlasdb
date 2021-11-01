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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ComplexPutUnlessExistsTable implements PutUnlessExistsTable {
    private final ConsensusForgettingPutUnlessExistsStore store;
    private final Function<byte[], byte[]> pendingValueTransformer;

    private ComplexPutUnlessExistsTable(
            ConsensusForgettingPutUnlessExistsStore store, Function<byte[], byte[]> pendingValueTransformer) {
        this.store = store;
        this.pendingValueTransformer = pendingValueTransformer;
    }

    public static PutUnlessExistsTable create(
            KeyValueService keyValueService,
            TableReference tableReference,
            Function<byte[], byte[]> pendingValueTransformer) {
        return new ComplexPutUnlessExistsTable(
                new ConsensusForgettingPutUnlessExistsStore(keyValueService, tableReference), pendingValueTransformer);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell c) {
        return Futures.transform(
                get(ImmutableSet.of(c)), map -> Optional.ofNullable(map.get(c)), MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> get(Iterable<Cell> cells) {
        ListenableFuture<Map<Cell, PutUnlessExistsState>> currentState =
                store.get(Streams.stream(cells).collect(Collectors.toSet()));
        return Futures.transform(
                currentState,
                state ->
                        KeyedStream.stream(state)
                                .filter(it -> it.commitState() == CommitState.COMMITTED)
                                .map(it -> it.value().asNewByteArray()).collectToMap(),
                MoreExecutors.directExecutor());
    }

    @Override
    public void putUnlessExists(Cell c, byte[] value) throws KeyAlreadyExistsException {
        putUnlessExistsMultiple(ImmutableMap.of(c, value));
    }

    @Override
    public void putUnlessExistsMultiple(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        try {
            store.putUnlessExists(KeyedStream.stream(values)
                    .map(userValue -> (PutUnlessExistsState) ImmutablePutUnlessExistsState.builder()
                            .value(Bytes.from(userValue))
                            .commitState(CommitState.PENDING)
                            .build())
                    .collectToMap());
            store.put(KeyedStream.stream(values)
                    .map(userValue -> (PutUnlessExistsState) ImmutablePutUnlessExistsState.builder()
                            .value(Bytes.from(userValue))
                            .commitState(CommitState.COMMITTED)
                            .build())
                    .collectToMap());
        } catch (KeyAlreadyExistsException e) {
            makeDecisionOnStates(e, values);
        }
    }

    private void makeDecisionOnStates(KeyAlreadyExistsException initialException, Map<Cell, byte[]> userValues) {
        // Acceptable: this call is part of a putUnlessExists, which is blocking API.
        Map<Cell, PutUnlessExistsState> existingStates = AtlasFutures.getUnchecked(store.get(
                initialException.getExistingKeys()));
        Set<Cell> alreadyCommittedValues = new HashSet<>();
        Map<Cell, byte[]> valuesToTryPutting = new HashMap<>();

        for (Map.Entry<Cell, PutUnlessExistsState> existingState : existingStates.entrySet()) {
            if (existingState.getValue().commitState() == CommitState.COMMITTED) {
                alreadyCommittedValues.add(existingState.getKey());
            } else {
                // Pending. We can try to overwrite this.
                byte[] userValue = userValues.get(existingState.getKey());
                valuesToTryPutting.put(existingState.getKey(), userValue);
            }
        }

        if (!alreadyCommittedValues.isEmpty()) {
            throw new KeyAlreadyExistsException(
                    initialException.getMessage(),
                    alreadyCommittedValues,
                    initialException.getKnownSuccessfullyCommittedKeys());
        }
        // We can try resolving decisions
        // No multi-CAS :(
        for (Map.Entry<Cell, byte[]> valueToTryPutting : valuesToTryPutting.entrySet()) {
            store.checkAndSet(valueToTryPutting.getKey(),
                    existingStates.get(valueToTryPutting.getKey()),
                    ImmutablePutUnlessExistsState.builder()
                            .value(Bytes.from(valueToTryPutting.getValue()))
                            .commitState(CommitState.PENDING)
                            .build());
            store.put(valueToTryPutting.getKey(),
                    ImmutablePutUnlessExistsState.builder()
                            .value(Bytes.from(valueToTryPutting.getValue()))
                            .commitState(CommitState.COMMITTED)
                            .build());
        }

        Map<Cell, byte[]> remainingUserValues = KeyedStream.stream(userValues)
                        .filterKeys(c -> !valuesToTryPutting.containsKey(c))
                .filterKeys(c -> !initialException.getKnownSuccessfullyCommittedKeys().contains(c))
                        .collectToMap();
        if (!remainingUserValues.isEmpty()) {
            putUnlessExistsMultiple(remainingUserValues);
        }
    }

    private byte[] makeDecisionOnState(Cell c, PutUnlessExistsState state) {
        if (state.commitState() == CommitState.COMMITTED) {
            return state.value().asNewByteArray();
        } else if (state.commitState() == CommitState.PENDING) {
            Bytes valueToWrite = Bytes.from(pendingValueTransformer.apply(state.toByteArray()));
            store.checkAndSet(
                    c,
                    state,
                    ImmutablePutUnlessExistsState.builder()
                            .value(valueToWrite)
                            .commitState(CommitState.PENDING)
                            .build());
            store.put(
                    c,
                    ImmutablePutUnlessExistsState.builder()
                            .value(valueToWrite)
                            .commitState(CommitState.COMMITTED)
                            .build());
            return valueToWrite.asNewByteArray();
        } else {
            throw new SafeIllegalStateException("Shouldn't be here?", SafeArg.of("commitState", state.commitState()));
        }
    }
}
