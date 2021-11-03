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
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import com.palantir.conjure.java.lib.Bytes;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ComplexPutUnlessExistsTable implements PutUnlessExistsTable {
    private final ConsensusForgettingPutUnlessExistsStore store;

    private ComplexPutUnlessExistsTable(
            ConsensusForgettingPutUnlessExistsStore store) {
        this.store = store;
    }

    public static PutUnlessExistsTable create(
            KeyValueService keyValueService,
            TableReference tableReference) {
        return new ComplexPutUnlessExistsTable(
                new ConsensusForgettingPutUnlessExistsStore(keyValueService, tableReference));
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
                this::resolvePendingDecisions,
                MoreExecutors.directExecutor()); // TODO (jkong): Naughty
    }

    @Override
    public void putUnlessExists(Cell c, byte[] value) throws KeyAlreadyExistsException {
        putUnlessExistsMultiple(ImmutableMap.of(c, value));
    }

    @Override
    public void putUnlessExistsMultiple(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
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
    }

    private Map<Cell, byte[]> resolvePendingDecisions(Map<Cell, PutUnlessExistsState> initialRawDbRead) {
        Map<Cell, PutUnlessExistsState> pendingCells = KeyedStream.stream(initialRawDbRead)
                .filter(read -> read.commitState() == CommitState.PENDING)
                .collectToMap();

        // TODO (jkong): Replace with multi-cas
        for (Map.Entry<Cell, PutUnlessExistsState> pendingCell : pendingCells.entrySet()) {
            store.checkAndSet(
                    pendingCell.getKey(),
                    pendingCell.getValue(),
                    ImmutablePutUnlessExistsState.builder()
                            .value(pendingCell.getValue().value())
                            .commitState(CommitState.PENDING)
                            .build());
            store.put(
                    pendingCell.getKey(),
                    ImmutablePutUnlessExistsState.builder()
                            .value(pendingCell.getValue().value())
                            .commitState(CommitState.COMMITTED)
                            .build());
        }

        return KeyedStream.stream(initialRawDbRead)
                .map(read -> read.value().asNewByteArray())
                .collectToMap();
    }
}
